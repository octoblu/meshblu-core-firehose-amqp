_              = require 'lodash'
async          = require 'async'
FirehoseWorker = require '../src/firehose-worker'
MeshbluAmqp    = require 'meshblu-amqp'
RedisNS        = require '@octoblu/redis-ns'
redis          = require 'ioredis'

describe 'create subscription', ->
  beforeEach (done) ->
    @redisClient = new RedisNS 'messages', redis.createClient()
    @redisClient.on 'ready', =>
      @redisClient.del 'subscriptions:amqp', done

  beforeEach ->
    @worker = new FirehoseWorker
      amqpUri: 'amqp://meshblu:judgementday@127.0.0.1'
      jobTimeoutSeconds: 1
      jobLogRedisUri: 'redis://localhost:6379'
      jobLogQueue: 'sample-rate:0.00'
      jobLogSampleRate: 0
      maxConnections: 10
      redisUri: 'redis://localhost:6379'
      namespace: 'messages'

    @worker.run (error) =>
      throw error if error?

  afterEach (done) ->
    @worker.stop done

  beforeEach (done) ->
    @client = new MeshbluAmqp uuid: 'some-uuid', token: 'some-token', hostname: 'localhost'
    @client.connect (error) =>
      return done error if error?
      @client.connectFirehose done

  it 'should insert the uuid into the subscription queue in redis', (done) ->
    @members = []
    checkList = (callback) =>
      @redisClient.lrange 'subscriptions:amqp', 0, -1, (error, @members) =>
        return callback error if error?
        callback()

    async.until (=> _.includes @members, @client.firehoseQueueName), checkList, (error) =>
      return done error if error?
      expect(@members).to.include @client.firehoseQueueName
      done()
