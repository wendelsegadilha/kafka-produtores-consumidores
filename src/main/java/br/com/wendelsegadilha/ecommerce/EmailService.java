package br.com.wendelsegadilha.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class EmailService {

    public static void main( String[] args ) throws ExecutionException, InterruptedException {
        var emailService = new EmailService();
        try(var service = new KafkaService("ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                EmailService.class.getSimpleName(),
                String.class,
                new HashMap<String, String>())){
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("========================================");
        System.out.println("Email new Order received");
        System.out.println(record.value());
        System.out.println("key: " + record.key() + "/ partition " + record.partition() + "/ offset " + record.offset() + "/ timestamp " + record.timestamp());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Send email");
    }

}
