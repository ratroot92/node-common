
const {Kafka}=require('kafka')



class KafkaConfig{
    kafka;
    clientId,
    brokers;
    producer;
    consumer;




    constructor(options){
        this.clientId=options.clientId;
        this.brokers=options.brokers;
        this.connect()
    }

    connect(){
        this.kafka=new Kafka({
            clientId=this.clientId,
            brokerst:this.brokers
        })
        this.producer=this.kafka.producer()
    }


    async registerServices(options={}){
        this.consumer=this.kafka.consumer({
            groupId:options.groupId+Math.random()})
           await  this.consumer.connect();
           for(const [key,service] of Object.entries(options.services)){
               for(const [key2,operations] of Object.entries(service.operations)){
                   const topic=`${key}.${key2}`;
                   await this.consumer.subscribe({topic:topic,fromBegininng:false})
               }
           }
           await this.consumer.run({
               eachMessage:async({topic,partition,message})=>{
                   try{
                    console.log("*** Event Received Start ***")
                    const key=message.key?.toString()
                    console.log("[key] => ",key)
                    const messageReceived=message.value?.toString()
                    console.log("[messageReceived] => ",messageReceived)
                    const payload=JSON.parse(messageReceived)
                    console.log("[payload] => ",payload)
                    const topic=topic.split(".")
                    console.log("[topic] => ",topic)
                    const serviceProviderName=options.services[topic[0]]?.provider;
                    console.log("[serviceProviderName] => ",serviceProviderName)
                    if(!serviceProviderName){
                        console.log("***- (!serviceProviderName) -***")
                        return 
                    }
                    else{
                        const serviceOperation=options.services[topic[0]].operations[topic[1]];
                        console.log("[serviceOperation] => ",serviceOperation)
                        const serviceProviderPath=path.resolve(__dirname,``,serviceProviderName)
                        console.log("[serviceProviderPath] => ",serviceProviderPath)
                        const serviceProvider=require(serviceProviderName);
                        console.log("[serviceProvider] => ",serviceProvider)
                        console.log("[payload] => ",payload)
                        console.log("[topic:] => ",`${topic[0]}.${topic[1]}`,)
                        serviceProvider[serviceOperation.handler](payload,{
                            topic:`${topic[0]}.${topic[1]}`,
                            key:key
                        })
                    }
                    console.log("*** Event Received End ***")

                   }
                   catch(err){

                   }
               }
           })
    }
}


module.exports=KafkaConfig