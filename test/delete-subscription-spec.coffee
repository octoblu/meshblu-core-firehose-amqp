_              = require 'lodash'
async          = require 'async'
FirehoseWorker = require '../src/firehose-worker'
MeshbluAmqp    = require 'meshblu-amqp'
RedisNS        = require '@octoblu/redis-ns'
redis          = require 'ioredis'

describe 'delete subscription', ->
  beforeEach (done) ->
    rawClient = redis.createClient(dropBufferSupport: true)
    @redisClient = new RedisNS 'test:firehose:amqp', rawClient
    @redisClient.on 'ready', =>
      @redisClient.keys '*', (error, keys) =>
        return done error if error?
        return done() if _.isEmpty keys
        rawClient.del keys..., done

  beforeEach ->
    @worker = new FirehoseWorker
      amqpUri: 'amqp://meshblu:judgementday@127.0.0.1'
      jobTimeoutSeconds: 1
      jobLogRedisUri: 'redis://localhost:6379'
      jobLogQueue: 'sample-rate:0.00'
      jobLogSampleRate: 0
      maxConnections: 10
      redisUri: 'redis://localhost:6379'
      namespace: 'test:firehose:amqp'
      hydrantNamespace: 'messages'

    @worker.run (error) =>
      throw error if error?

  afterEach (done) ->
    @worker.stop done

  beforeEach (done) ->
    @client = new MeshbluAmqp uuid: 'some-uuid', token: 'some-token', hostname: 'localhost'
    @client.connect (error) =>
      return done error if error?
      @client.connectFirehose done

  beforeEach (done) ->
    @members = []
    checkList = (callback) =>
      @redisClient.lrange 'subscriptions', 0, -1, (error, @members) =>
        return callback error if error?
        callback()
    async.until (=> _.includes @members, @client.firehoseQueueName), checkList, (error) =>
      return done error if error?
      @client.disconnectFirehose done

  it 'should remove the uuid from the subscription queue in redis', (done) ->
    checkList = (callback) =>
      @redisClient.lrange 'subscriptions', 0, -1, (error, @members) =>
        return callback error if error?
        callback()

    async.until (=> !_.includes @members, @client.firehoseQueueName), checkList, (error) =>
      return done error if error?
      expect(@members).to.not.include @client.firehoseQueueName
      done()
