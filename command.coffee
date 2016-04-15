_      = require 'lodash'
FirehoseWorker = require './src/firehose-worker'

class Command
  constructor: ->
    @options =
      amqpUri          : process.env.AMQP_URI
      aliasServerUri   : process.env.ALIAS_SERVER_URI
      redisUri         : process.env.REDIS_URI
      namespace        : process.env.NAMESPACE || 'firehose:amqp'
      hydrantNamespace : process.env.HYDRANT_NAMESPACE || 'messages'

  panic: (error) =>
    console.error error.stack
    process.exit 1

  run: =>
    @panic new Error('Missing required environment variable: ALIAS_SERVER_URI') unless @serverOptions.aliasServerUri? # allowed to be empty
    @panic new Error('Missing required environment variable: AMQP_URI') if _.isEmpty @options.amqpUri
    @panic new Error('Missing required environment variable: REDIS_URI') if _.isEmpty @options.redisUri

    worker = new FirehoseWorker @options

    console.log 'AMQP firehose worker is working'

    worker.run (error) =>
      return @panic error if error?

    process.on 'SIGTERM', =>
      console.log 'SIGTERM caught, exiting'
      worker.stop =>
        process.exit 0

command = new Command()
command.run()
