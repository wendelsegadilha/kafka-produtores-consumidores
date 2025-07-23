package br.com.wendelsegadilha.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    public static void main( String[] args ) throws ExecutionException, InterruptedException {

        var fraudDetectorService = new FraudDetectorService();
        try(var service = new KafkaService<Order>(
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                FraudDetectorService.class.getSimpleName(), Order.class,
                new HashMap<String, String>())){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("========================================");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("key: " + record.key() + "/ partition " + record.partition() + "/ offset " + record.offset() + "/ timestamp " + record.timestamp());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Order processed");
    }

}
