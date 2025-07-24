package br.com.wendelsegadilha;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class LogService {

    public static void main( String[] args ) throws ExecutionException, InterruptedException {

        var logService = new LogService();
        try(var service = new KafkaService(
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                LogService.class.getSimpleName(),
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))){
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("========================================");
        System.out.println("LOG received: " + record.topic());
        System.out.println(record.value());
        System.out.println("key: " + record.key() +  "/ partition " + record.partition() + "/ offset " + record.offset() + "/ timestamp " + record.timestamp());
    }

}

