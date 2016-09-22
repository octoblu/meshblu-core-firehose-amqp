_                   = require 'lodash'
{Client}            = require 'amqp10'
Promise             = require 'bluebird'
debug               = require('debug')('meshblu-core-worker-amqp:worker')
RedisNS             = require '@octoblu/redis-ns'
redis               = require 'ioredis'
MultiHydrantFactory = require 'meshblu-core-manager-hydrant/multi'
UuidAliasResolver   = require 'meshblu-uuid-alias-resolver'
Redlock             = require 'redlock'
async               = require 'async'
UUID                = require 'uuid'

class FirehoseWorker
  constructor: (options={}, dependencies={}) ->
    {
      @aliasServerUri
      @amqpUri
      @maxConnections
      @redisUri
      @firehoseRedisUri
      @namespace
      @hydrantNamespace
    } = options
    throw new Error('FirehoseWorker: @hydrantNamespace is required') unless @hydrantNamespace?
    throw new Error('FirehoseWorker: @namespace is required') unless @namespace?
    @UUID = dependencies.UUID ? UUID

  connect: (callback) =>
    @subscriptions = {}
    @subscriptionLookup = {}
    @redisClient = new RedisNS @namespace, redis.createClient(@redisUri, dropBufferSupport: true)
    @queueClient = new RedisNS @namespace, redis.createClient(@redisUri, dropBufferSupport: true)
    hydrantClient = new RedisNS @hydrantNamespace, redis.createClient(@firehoseRedisUri, dropBufferSupport: true)
    uuidAliasClient = new RedisNS 'uuid-alias', redis.createClient(@redisUri, dropBufferSupport: true)

    redlockOptions =
      retryCount: 100
      retryDelay: 100
    @redlock = new Redlock [@queueClient], redlockOptions

    uuidAliasResolver = new UuidAliasResolver
      cache: uuidAliasClient
      aliasServerUri: @aliasServerUri

    @hydrant = new MultiHydrantFactory {client: hydrantClient, uuidAliasResolver}
    @hydrant.on 'message', @_onHydrantMessage
    @hydrant.connect (error) =>
      return callback(error) if error?
      @_connectAmqp(callback)

  _connectAmqp: (callback) =>
    options =
      reconnect:
        forever: false
        retries: 0

    @client = new Client options
    @client.connect @amqpUri
      .then =>
        @client.once 'connection:closed', =>
          throw new Error 'connection to amqp server lost'
        Promise.all [
          @client.createSender()
          @client.createReceiver('meshblu.firehose.request')
        ]
      .spread (@sender, @receiver) =>
        callback()
        @receiver.on 'message', @_onMessage
        return true # promises are dumb
      .catch (error) =>
        callback error
      .error (error) =>
        callback error

  run: (callback) =>
    @connect (error) =>
      return callback error if error?
      @_processQueueForever()

  stop: (callback) =>
    @stopped = true
    @hydrant?.close()
    @client.disconnect()
      .then callback
      .catch callback

  add: (subscriptionObj, callback) =>
    {
      subscription
    } = subscriptionObj

    debug 'add', subscription

    nonce = @UUID.v4()
    subscriptionObj.nonce = nonce

    tasks = [
      async.apply @redisClient.set, "data:#{subscription}", JSON.stringify(subscriptionObj)
      async.apply @redisClient.lrem, 'subscriptions', 0, subscription
      async.apply @redisClient.rpush, 'subscriptions', subscription
    ]

    async.series tasks, callback

  remove: (subscriptionObj, callback) =>
    {
      subscription
    } = subscriptionObj

    debug 'remove', subscription

    tasks = [
      async.apply @redisClient.del, "data:#{subscription}"
      async.apply @redisClient.lrem, 'subscriptions', 0, subscription
      async.apply @_unclaimSubscription, subscriptionObj.subscription
    ]

    async.series tasks, callback

  _processQueueForever: =>
    async.forever @_processQueue, (error) =>
      throw error if error? && !@stopped

  _processQueue: (callback) =>
    @queueClient.brpoplpush 'subscriptions', 'subscriptions', 30, (error, subscription) =>
      return callback new Error('stopping') if @stopped
      return callback error if error?
      return callback() unless subscription?

      @_acquireLock subscription, (error) =>
        return callback error if error?
        setTimeout callback, 100

  _acquireLock: (subscription, callback) =>
    @redlock.lock "locks:#{subscription}", 60*1000, (error, lock) =>
      return callback() if error?
      return callback() unless lock?
      @_handleSubscription subscription, (error) =>
        lock.unlock (lockError) =>
          return callback lockError if lockError?
          callback error

  _handleSubscription: (subscription, callback) =>
    @_checkClaimableSubscription subscription, (error, claimable) =>
      return callback error if error?
      return callback() unless claimable
      async.series [
        async.apply @_claimSubscription, subscription
        async.apply @_createSubscription, subscription
        async.apply @_destroySubscription, subscription
      ], callback

  _checkClaimableSubscription: (subscription, callback) =>
    @redisClient.exists "claim:#{subscription}", (error, exists) =>
      return callback error if error?
      return callback null, true if exists == 0
      return callback null, @_isSubscribed(subscription)

  _claimSubscription: (subscription, callback) =>
    @redisClient.setex "claim:#{subscription}", 60, Date.now(), callback

  _unclaimSubscription: (subscription, callback) =>
    debug '_unclaimSubscription', subscription

    delete @subscriptions[subscription]
    @_unsubscribeHydrant subscription, (error) =>
      return callback error if error?
      @redisClient.del "claim:#{subscription}", callback

  _getSubscription: (subscription, callback) =>
    @redisClient.get "data:#{subscription}", (error, data) =>
      return callback error if error?
      try
        subscriptionObj = JSON.parse(data)
      catch error
        return callback error

      callback null, subscriptionObj

  _checkNonce: (subscriptionObj, callback) =>
    return callback null, false

  _createSubscription: (subscription, callback) =>
    return callback() if @_isSubscribed subscription
    @_getSubscription subscription, (error, subscriptionObj) =>
      return callback error if error?
      return callback 'Not Found' unless subscriptionObj?
      @_subscribeHydrant subscription, (error) =>
        return callback error if error?
        @subscriptions[subscription] = subscriptionObj.nonce
        callback()

  _destroySubscription: (subscription, callback) =>
    return callback() unless @_isSubscribed subscription
    @_getSubscription subscription, (error, subscriptionObj) =>
      return callback error if error?
      return callback() if subscriptionObj?.nonce? && @subscriptions[subscription] == subscriptionObj?.nonce
      @_unclaimSubscription subscription, (error) =>
        return callback error if error?
        delete @subscriptions[subscription]
        callback()

  _isSubscribed: (subscription) =>
    @subscriptions[subscription]?

  _subscribeHydrant: (subscription, callback) =>
    return callback() if @_isSubscribed subscription
    uuid = _.first subscription.split /\./
    @hydrant.subscribe {uuid}, (error) =>
      return callback error if error?
      @subscriptionLookup[uuid] ?= []
      @subscriptionLookup[uuid].push subscription
      callback()

  _unsubscribeHydrant: (subscription, callback) =>
    uuid = _.first subscription.split /\./
    @hydrant.unsubscribe {uuid}, (error) =>
      return callback error if error?
      _.pull @subscriptionLookup.uuid, subscription
      callback()

  _onMessage: (message) =>
    {replyTo} = message.properties
    {jobType} = message.applicationProperties

    if jobType == 'DisconnectFirehose'
      @remove subscription: replyTo, (error) =>
        throw error if error?

    if jobType == 'ConnectFirehose'
      @add subscription: replyTo, (error) =>
        throw error if error?

  _onHydrantMessage: (channel, message) =>
    { rawData } = message
    rawData ?= {}

    _.each @subscriptionLookup[channel], (subject) =>
      options =
        properties:
          subject: subject
        applicationProperties: message.metadata

      @sender.send rawData, options

module.exports = FirehoseWorker
