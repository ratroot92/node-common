// const { KafkaConfig } = require("./kafkaConfig");

// class KafkaStream extends KafkaConfig {
//   constructor(options) {
//     super(options);
//   }

//   async produceMessage(payload = {}, options = {}) {
//     try {
//       const pload = JSON.stringify(payload);
//       this.producer = this.kafka.producer();
//       const kafkaRes = await this.producer.send({
//         topic: options.topic,
//         messages: [
//           {
//             key: options.key ?? null,
//             value: pload,
//           },
//         ],
//         acks: options.acks ?? -1,
//         timeout: options.timeout ?? 3000,
//       });
//       await this.producer.disconnect();
//       return kafkaRes;
//     } catch (err) {
//       console.log(`error at produceMessage \n ${err.message}`);
//     }
//   }

//   async consume(options = {}) {
//     await this.consumer.connect();
//     await this.consumer.subscribe({
//       topic: options.topic,
//       fromBegining: options.fromBegining ?? true,
//     });
//     await this.consumer.run({
//       eachMessage: async ({ topic, partition, message }) => {
//         console.log({
//           value: message.value.toString(),
//         });
//       },
//     });
//     await this.consumer.disconnect();
//   }
// }

// module.exports = { KafkaStream };

const { Kafka, logLevel } = require("kafkajs");
class KafkaStream {
  kafka;
  kafkaAdmin;
  brokers;
  kafkaProducer;
  kafkaConsumer;

  constructor(options = {}) {
    try {
      this.kafka = new Kafka({
        clientId: options.clientId,
        brokers: options.brokers,
        logLevel: logLevel.NOTHING,
      });
      this.kafkaAdmin = this.kafka.admin();
      this.kafkaProducer = this.kafka.producer();
      this.kafkaConsumer = this.kafka.consumer({ groupId: options.groupId });
    } catch (err) {
      console.log(err.stack);
    }
  }

  listen() {
    this.kafka.kafkaProducer.on("event", function () {
      console.log("event");
    });
  }

  async deleteTopics(topicName) {
    try {
      if (topicName === "" || topicName === undefined)
        throw new Error("topicName name is required.");

      await kafkaAdmin.connect();
      const topicsList = await kafkaAdmin.listTopics();

      if (topicsList.includes(topicName) === true) {
        await kafkaAdmin.deleteTopics({
          topics: [topicName],
        });
      }
      await kafkaAdmin.disconnect();
      return true;
    } catch (err) {
      throw new Error(err.stack);
    }
  }

  async getTopicsList() {
    await this.kafkaAdmin.connect();
    const topicList = await this.kafkaAdmin.listTopics();
    await this.kafkaAdmin.disconnect();
    return topicList;
  }

  async adminCreateTopics(topicName) {
    try {
      if (topicName === "" || topicName === undefined)
        throw new Error("topicName name is required.");

      await this.kafkaAdmin.connect();
      const topicsList = await this.kafkaAdmin.listTopics();
      if (topicsList.includes(topicName) === false) {
        await this.kafkaAdmin.createTopics({
          waitForLeaders: true,
          topics: [{ topic: topicName }],
        });
      } else {
        // throw new Error('topic already exist')
      }
      await this.kafkaAdmin.disconnect();
      return true;
    } catch (err) {
      throw new Error(err.stack);
    }
  }

  async sendKafkaMessage(payload = {}, options = {}) {
    try {
      await this.adminCreateTopics(options.topic);
      payload = JSON.stringify(payload);

      await this.kafkaProducer.connect();
      const kafkaRes = await this.kafkaProducer.send({
        topic: options.topic,
        messages: [
          {
            key: options.key ?? null,
            value: payload,
          },
        ],
        acks: options.acks ?? -1,
        timeout: options.timeout ?? 3000,
      });
      await this.kafkaProducer.disconnect();
      console.log("message sent successfully");
      return kafkaRes;
    } catch (err) {
      throw new Error(err.stack);
    }
  }
  async startKafkaConsumer(groupId, topicName) {
    try {
      if (topicName === "" || topicName === undefined)
        throw new Error("topicName name is required.");
      if (groupId === "" || groupId === undefined)
        throw new Error("groupId name is required.");

      this.kafkaConsumer = this.kafka.consumer({ groupId });
      await this.kafkaConsumer.connect();
      await this.kafkaConsumer.subscribe({
        topic: topicName,
        fromBeginning: true,
      });
      await this.kafkaConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          console.log(`Received To : [${process.env.SERVICE_NAME}]`);
          const payload = JSON.parse(message.value);
          console.log("type ==>", payload);
          // console.log("data ==>", data);
          // console.log("topic ==>", topic);
          // console.log("partition ==>", partition);
          // kafkaActions[actionType]()
        },
      });
    } catch (err) {
      throw new Error(err.stack);
    }
  }
}

module.exports = { KafkaStream };
