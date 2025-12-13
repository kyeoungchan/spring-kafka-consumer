package com.example.springkafkaconsumer.customlistenercontainer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class ListenerContainerConfiguration {

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> customContainerFactory() {

        Map<String, Object> props = new HashMap<>();

        // group.id는 리스너 컨테이너에도 선언하므로 여기에서 선언하지 않아도 된다.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 컨슈머 옵션값을 파라미터로 받는 DefaultKafkaConsumerFactory 인스턴스 생성
        // 리스너 컨테이너 팩토리를 생성할 때 컨슈머 기본 옵션을 설정하는 용도로 사용된다.
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(props);

        /* ConcurrentKafkaListenerContainerFactory는 리스너 컨테이너를 만들기 위해 사용된다.
         * 2개 이상의 컨슈머 리스너를 만들 때 사용된다.
         * concurrency를 1로 설정할 경우, 1개 컨슈머 스레드로 실행된다. */
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();

        // 리밸런스 리스너를 선언하기 위해 setConsumerRebalanceListener() 메서드 호출
        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {

            // 커밋이 되기 전에 리밸런스가 발생했을 때 호출
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            }

            // 커밋이 일어난 후 리밸런스가 발생했을 때 호출
            @Override
            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
            }
        });

        factory.setBatchListener(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setConsumerFactory(cf);
        return factory;
    }
}
