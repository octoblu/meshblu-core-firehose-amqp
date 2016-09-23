_              = require 'lodash'
async          = require 'async'
FirehoseWorker = require '../src/firehose-worker'
MeshbluAmqp    = require 'meshblu-amqp'
RedisNS        = require '@octoblu/redis-ns'
redis          = require 'ioredis'

describe 'connect firehose subscription', ->
  beforeEach (done) ->
    rawClient = redis.createClient(dropBufferSupport: true)
    @redisClient = new RedisNS 'test:firehose:amqp', rawClient
    @redisClient.on 'ready', =>
      @redisClient.keys '*', (error, keys) =>
        return done error if error?
        return done() if _.isEmpty keys
        rawClient.del keys..., done

  beforeEach (done) ->
    @redisHydrantClient = new RedisNS 'messages', redis.createClient(dropBufferSupport: true)
    @redisHydrantClient.on 'ready', done

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
    doneTwice = _.after 2, done
    message =
      metadata:
        route: [{toUuid: 'a', fromUuid: 'b', type: 'message.sent'}]
      rawData: '{"foo":"bar"}'

    @members = []
    @subscriptionExists = false
    checkList = (callback) =>
      @redisClient.lrange 'subscriptions', 0, -1, (error, @members) =>
        return callback error if error?
        callback()

    @client.once 'message', (@message) =>
      doneTwice()

    async.until (=> _.includes @members, @client.firehoseQueueName), checkList, (error) =>
      return done error if error?
      async.until (=> @worker.subscriptions?[@client.firehoseQueueName]?), ((cb) => setTimeout(cb, 100)), (error) =>
        return done error if error?
        @redisHydrantClient.publish 'some-uuid', JSON.stringify(message), (error, published) =>
          return done error if error?
          return done(new Error 'failed to publish') if published == 0
          doneTwice()

  it 'should emit a message', ->
    expectedMetadata =
      route: [
        fromUuid: 'b'
        toUuid: 'a'
        type: 'message.sent'
      ]
    expect(@message.metadata).to.deep.equal expectedMetadata
    expect(@message.data).to.deep.equal foo:"bar"
