package br.com.wendelsegadilha;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
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

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("key: " + record.key() + "/ partition " + record.partition() + "/ offset " + record.offset() + "/ timestamp " + record.timestamp());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        var order = record.value();
        if(isFraud(order)) {
            System.out.println("Order is a fraud!!!" + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        } else {
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
        }

    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
