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

class FirehoseWorker
  constructor: (options)->
    {@aliasServerUri, @amqpUri, @maxConnections, @redisUri, @namespace, @hydrantNamespace} = options
    throw new Error('FirehoseWorker: @hydrantNamespace is required') unless @hydrantNamespace?
    throw new Error('FirehoseWorker: @namespace is required') unless @namespace?

  connect: (callback) =>
    @subscriptions = {}
    @subscriptionLookup = {}
    @redisClient = new RedisNS @namespace, redis.createClient(@redisUri)
    @queueClient = new RedisNS @namespace, redis.createClient(@redisUri)
    hydrantClient = new RedisNS @hydrantNamespace, redis.createClient(@redisUri)
    uuidAliasClient = new RedisNS 'uuid-alias', redis.createClient(@redisUri)

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
      return callback error if error?
      return callback() unless lock?
      @_handleSubscription subscription, (error) =>
        lock.unlock()
        callback error

  _handleSubscription: (subscription, callback) =>
    @_checkClaimableSubscription subscription, (error, claimable) =>
      return callback error if error?
      return callback() unless claimable
      async.series [
        async.apply @_claimSubscription, subscription
        async.apply @_subscribeHydrant, subscription
        async.apply @_updateSubscriptions, subscription
      ], callback

  _checkClaimableSubscription: (subscription, callback) =>
    @redisClient.exists subscription, (error, exists) =>
      return callback error if error?
      claimable = @_isSubscribed(subscription) or !exists
      return callback null, claimable

  _claimSubscription: (subscription, callback) =>
    @redisClient.setex subscription, Date.now(), 60, (error) =>
      return callback error if error?
      callback()

  _subscribeHydrant: (subscription, callback) =>
    return callback() if @_isSubscribed subscription
    uuid = _.first subscription.split /\./
    @hydrant.subscribe {uuid}, (error) =>
      return callback error if error?
      callback()

  _updateSubscriptions: (subscription, callback) =>
    return callback() if @_isSubscribed subscription
    uuid = _.first subscription.split /\./
    @subscriptionLookup[uuid] ?= []
    @subscriptionLookup[uuid].push subscription
    @subscriptions[subscription] = true
    callback()

  _isSubscribed: (subscription) =>
    @subscriptions[subscription]?

  _onMessage: (message) =>
    {replyTo} = message.properties
    {jobType} = message.applicationProperties

    if jobType == 'DisconnectFirehose'
      return @redisClient.lrem 'subscriptions', 0, replyTo, (error) =>
        throw error if error?

    if jobType == 'ConnectFirehose'
      @redisClient.lrem 'subscriptions', 0, replyTo, (error) =>
        throw error if error?
        @redisClient.rpush 'subscriptions', replyTo, (error) =>
          throw error if error?

  _onHydrantMessage: (channel, message) =>
    _.each @subscriptionLookup[channel], (subject) =>
      options =
        properties:
          subject: subject
        applicationProperties: message.metadata

      @sender.send message.rawData || {}, options

module.exports = FirehoseWorker
