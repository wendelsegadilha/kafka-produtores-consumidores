package br.com.wendelsegadilha.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrder {

    public static void main( String[] args ) throws ExecutionException, InterruptedException {

        var producer = new KafkaProducer<String, String>(properties());
        var value = "123,456,600";
        var topic = "ECOMMERCE_NEW_ORDER";
        var record = new ProducerRecord<String, String>(topic, value, value);
        producer.send(record, (data, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
                return;
            }
            System.out.println(data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        }).get();

    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.236.87:9092"); //usar ip do wsl (ip addr | grep inet)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
