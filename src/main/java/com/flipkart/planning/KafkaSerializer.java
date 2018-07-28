package com.flipkart.planning;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@NoArgsConstructor
public class KafkaSerializer implements Serializer {
    private static final ObjectMapper objectMapper = new ObjectMapper();


    public void configure(Map configs, boolean isKey) {

    }


    public byte[] serialize(String topic, Object data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing class " + data.getClass().toString() + " to JSON", e);
        }
    }


    public void close() {

    }
}
