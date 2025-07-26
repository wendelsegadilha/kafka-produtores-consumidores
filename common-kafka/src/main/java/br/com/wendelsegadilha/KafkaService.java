package br.com.wendelsegadilha;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;
    private final String groupId;
    private Class<T> type;
    private Map<String, String> properties;

    public KafkaService(ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
        this.parse = parse;
        this.groupId = groupId;
        this.type = type;
        this.properties = properties;
        this.consumer = new KafkaConsumer<>(properties());
    }

    public KafkaService(String topic, ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
        this(parse, groupId, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(Pattern topic, ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
        this(parse, groupId, type, properties);
        consumer.subscribe(topic);
    }

    private Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.236.87:9092"); //usar ip do wsl (ip addr | grep inet)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // RECEBE DE 1 EM 1 COMITANDO SEMPRE
        // EVITA PROBLEMAS DE PERDA DE MENSAGENS E REPROCESSAMENTO POR FALHA DE COMMIT DEVIDO AO REBALANCEAMENTO
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(this.properties);
        return properties;
    }

    public void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                records.forEach(record -> {
                    try {
                        parse.consumer(record);
                    } catch (Exception e) {
                        //somente logamos por enquanto
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
