package br.com.wendelsegadilha;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder {

    public static void main( String[] args ) throws ExecutionException, InterruptedException {

        try(var oderDispatcher = new KafkaDispatcher<Order>(); var emailDispatcher = new KafkaDispatcher<String>()){
            var email = Math.random() + "@gmail.com";
            for (int i = 0; i < 10; i++) {
                var oderId = UUID.randomUUID().toString();
                var amount = new BigDecimal(Math.random() * 5000 + 1);
                var order = new Order(oderId, amount, email);
                oderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

                var emailContent = "Pedido de compra em procesamento";
                emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailContent);
            }
        }

    }

}
