package br.com.wendelsegadilha.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    public static void main( String[] args ) throws ExecutionException, InterruptedException {

        var consumer = new KafkaConsumer<String, String>(properties());
        var topic = "ECOMMERCE_NEW_ORDER";
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                records.forEach(record -> {
                    System.out.println("========================================");
                    System.out.println("Processing new order, checking for fraud");
                    System.out.println("key: " + record.key() + "/ partition " + record.partition() + "/ offset " + record.offset() + "/ timestamp " + record.timestamp());

                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    System.out.println("Order processed");
                });
            }
        }

    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.236.87:9092"); //usar ip do wsl (ip addr | grep inet)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
        return properties;
    }

}
