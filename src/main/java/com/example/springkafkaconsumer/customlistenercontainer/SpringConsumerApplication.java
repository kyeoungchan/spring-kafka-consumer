package com.example.springkafkaconsumer.customlistenercontainer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
@SpringBootApplication
public class SpringConsumerApplication {
    public static void main(String[] args) {
        new SpringApplication(SpringConsumerApplication.class).run(args);
    }

    @KafkaListener(topics = "test",
            groupId = "test-group",
            // 빈 객체로 등록한 이름인 customContainerFactory로 설정한다.
            containerFactory = "customContainerFactory")
    public void customListener(String data) {
        log.info("data: {}", data);
    }
}
