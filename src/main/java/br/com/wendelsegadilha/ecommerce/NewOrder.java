package br.com.wendelsegadilha.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder {

    public static void main( String[] args ) throws ExecutionException, InterruptedException {

        try(var dispatcher = new KafkaDispatcher()){
            for (int i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();
                var value = key + ",456,600";
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Pedido de compra em procesamento";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }

    }

}
