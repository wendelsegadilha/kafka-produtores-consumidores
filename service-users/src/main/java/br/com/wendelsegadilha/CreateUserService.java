package br.com.wendelsegadilha;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
        var url = "jdbc:sqlite:target/users_database.db";
        this.connection = DriverManager.getConnection(url);
        var createTable = "create table users (" +
                "uuid varchar(200) primary key," +
                "email varchar(200))";
        try {
            connection.createStatement().execute(createTable);
        }catch (SQLException ex){
            // tomar cuidado com isso
        }
    }

    public static void main( String[] args ) throws ExecutionException, InterruptedException, SQLException {
        var createUserService = new CreateUserService();
        try(var service = new KafkaService<Order>(
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                CreateUserService.class.getSimpleName(), Order.class,
                new HashMap<String, String>())){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) throws SQLException {
        System.out.println("========================================");
        System.out.println("Processing new order, checking for new user");
        Order order = record.value();
        System.out.println("Oder: " + order);
        // salvar no banco
        if(isNewUser(order.getEmail())) {
            insertNewUser(order.getUserId(),order.getEmail());
        }

    }

    private void insertNewUser(String uuid, String email) throws SQLException {
        PreparedStatement insert = connection.prepareStatement("insert into users (uuid, email) values (?, ?)");
        insert.setString(1, uuid);
        insert.setString(2, email);
        insert.execute();
        System.out.println("Usuário uuid e email: " + email + " salvo com sucesso");
    }

    private boolean isNewUser(String email) throws SQLException {
        var exists = connection.prepareStatement("select uuid from users where email = ? limit 1");
        exists.setString(1, email);
        var results = exists.executeQuery();
        return !results.next();
    }

}
