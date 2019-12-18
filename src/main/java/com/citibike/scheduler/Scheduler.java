package com.citibike.scheduler;


import com.citibike.Application;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Properties;

@Component
public class Scheduler {
    private static Logger logger = LoggerFactory.getLogger(Application.class);
    public static String KAFKA_BROKERS = "localhost:9092";
    public static String url = "https://api.citybik.es/v2/networks/citycycle";


    @Scheduled(cron = "*/10 * * * * *")
    public void scheduledProducer() {
        RestTemplate template = new RestTemplate();
        HttpEntity<String> response = template.exchange(url, HttpMethod.GET, HttpEntity.EMPTY, String.class);
        logger.info(response.getBody());


        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Producer kProducer = new KafkaProducer<>(props);
        kProducer.send(new ProducerRecord<String, String>("citibike", "", response.getBody()));
    }
}
