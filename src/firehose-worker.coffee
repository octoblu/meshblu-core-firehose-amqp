{Client} = require 'amqp10'
Promise  = require 'bluebird'
debug    = require('debug')('meshblu-core-worker-amqp:worker')
RedisNS  = require '@octoblu/redis-ns'
redis    = require 'ioredis'

class FirehoseWorker
  constructor: (options)->
    {@aliasServerUri, @amqpUri, @maxConnections, @redisUri, @namespace} = options
    @subscriptions = {}

  connect: (callback) =>
    @redisClient = new RedisNS @namespace, redis.createClient(@redisUri)
    @redisClient.on 'ready', (error) =>
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

  stop: (callback) =>
    @client.disconnect()
      .then callback
      .catch callback

  _onMessage: (message) =>
    if message.applicationProperties.jobType == 'DisconnectFirehose'
      return @redisClient.lrem 'subscriptions:amqp', 0, message.properties.replyTo, (error) =>
        throw error if error?

    if message.applicationProperties.jobType == 'ConnectFirehose'
      @redisClient.lrem 'subscriptions:amqp', 0, message.properties.replyTo, (error) =>
        throw error if error?
        @redisClient.rpush 'subscriptions:amqp', message.properties.replyTo, (error) =>
          throw error if error?

      # redisClient = new RedisNS 'messages', redis.createClient()
      # @hydrant = new HydrantManager {client: redisClient, @uuidAliasResolver}
      # return @hydrant.connect uuid: message.applicationProperties.toUuid, (error) =>
    #     throw error if error?
    #     @hydrantConnected = true
    #     @hydrant.on 'message', (redisMessage) =>
    #       options =
    #         properties:
    #           subject: message.properties.replyTo
    #           correlationId: message.properties.correlationId
    #
    #       @sender.send redisMessage, options
    #
    #   return @hydrant.close()

module.exports = FirehoseWorker
