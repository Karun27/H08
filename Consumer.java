package com.spnotes.kafka.simple;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by sunilpatil on 12/28/15.
 */
public class Consumer {
    private static Scanner in;

    public static void main(String[] argv)throws Exception{
        if (argv.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n",
                    Consumer.class.getSimpleName());
            System.exit(-1);
        }
        in = new Scanner(System.in);
        String topicName = argv[0];
        String groupId = argv[1];

        ConsumerThread consumerRunnable = new ConsumerThread(topicName,groupId);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread{
        private String topicName;
        private String groupId;
        private KafkaConsumer<String,String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId){
            this.topicName = topicName;
            this.groupId = groupId;
        }
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            //Start processing messages
            String[] cars=new String[20];
            cars[0]="Alfa Romeo";
            cars[1]="Bentley";
            cars[2]="Chiron";
            cars[3]="Datsun";
            cars[4]="Explorer";
            cars[5]="Ford";
            cars[6]="Golardo";
            cars[7]="Honda";
            cars[8]="Infiniti";
            cars[9]="Jaguar";
            cars[10]="Kia";
            cars[11]="Lamborghini"
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records)
                    {
                        if(record.value().equalsIgnoreCase("A"))
                            System.out.println("You choose "+str[0]+" car");
                        else if(record.value().equalsIgnoreCase("B"))
                            System.out.println("You choose "+str[1]+" car");
                        else if(record.value().equalsIgnoreCase("C"))
                            System.out.println("You choose "+str[2]+" car");
                        else if(record.value().equalsIgnoreCase("D"))
                            System.out.println("You choose "+str[3]+" car");
                        else if(record.value().equalsIgnoreCase("E"))
                            System.out.println("You choose "+str[4]+" car");
                        else if(record.value().equalsIgnoreCase("F"))
                            System.out.println("You choose "+str[5]+" car");
                        else if(record.value().equalsIgnoreCase("G"))
                            System.out.println("You choose "+str[6]+" car");
                        else if(record.value().equalsIgnoreCase("H"))
                            System.out.println("You choose "+str[7]+" car");
                        else if(record.value().equalsIgnoreCase("I"))
                            System.out.println("You choose "+str[8]+" car");
                        else if(record.value().equalsIgnoreCase("J"))
                            System.out.println("You choose "+str[9]+" car");
                        else if(record.value().equalsIgnoreCase("K"))
                            System.out.println("You choose "+str[10]+" car");
                        else if(record.value().equalsIgnoreCase("L"))
                            System.out.println("You choose "+str[11]+" car");
                        else
                            System.out.println("Select a value from A-L");
                            
                    }
                        //System.out.println(record.value());
                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            }finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }
        public KafkaConsumer<String,String> getKafkaConsumer(){
           return this.kafkaConsumer;
        }
    }
}

