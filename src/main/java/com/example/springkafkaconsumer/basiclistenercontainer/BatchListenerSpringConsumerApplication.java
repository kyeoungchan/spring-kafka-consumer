package com.example.springkafkaconsumer.basiclistenercontainer;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
@SpringBootApplication
public class BatchListenerSpringConsumerApplication {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(BatchListenerSpringConsumerApplication.class);
        application.run(args);
    }

    /**
     * @param records 컨슈머 레코드의 묶음(ConsumerRecords)
     * 카프카 클라이언트 라이브러리에서 poll() 메서드로 리턴받은 ConsumerRecords를 리턴받아 사용하는 것과 동일하다.
     */
    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void batchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> log.info("batchListener record: {}", record.toString()));
    }

    /**
     * 메시지 값들을 List 자료구조로 받아서 처리한다.
     */
    @KafkaListener(topics = "test", groupId = "test-group-02")
    public void listBatchListener(List<String> list) {
        list.forEach(recordValue -> log.info("listBatchListener record: {}", recordValue));
    }

    /**
     * 2개 이상의 카프카 컨슈머 스레드를 실행하고 싶다면 concurrency 옵션을 사용하면 된다.
     * concurrency 옵션값에 해당하는 만큼 컨슈머 스레들르 만들어서 병렬처리한다.
     */
    @KafkaListener(topics = "test",
            groupId = "test-group-03",
            concurrency = "3")
    public void concurrentTopicListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> log.info("concurrentTopicListener record: {}", record.toString()));
    }
}
