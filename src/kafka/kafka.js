const KafkaConfig = require('./kafkaConfig')

class KafkaStream extends KafkaConfig {
    constructor(options) {
        super(options)
    }

    async produceMessage(payload = {}, options = {}) {
        const pload = JSON.stringify(payload)
        await this.producer.connect()
        const kafkaRes = await this.producer.send({
            topic: options.topic,
            messages: [
                {
                    key: options.key ?? null,
                    value: pload,
                },
            ],
            acks: options.acks ?? -1,
            timeout: options.timeout ?? 3000,
        })
        await this.producer.disconnect()
        return kafkaRes
    }

    async consume(options = {}) {
        await this.consumer.connect()
        await this.consumer.subscribe({ topic: options.topic, fromBegining: options.fromBegining ?? true })
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log({
                    value: message.value.toString(),
                })
            },
        })
        await this.consumer.disconnect()
    }
}

module.exports = { KafkaStream }
