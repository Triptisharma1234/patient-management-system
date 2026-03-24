package com.pm.billingservice.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import patient.events.PatientEvent;

@Service
public class KafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "patient", groupId = "billing-group")
    public void consume(byte[] data) {
        try {
            PatientEvent event = PatientEvent.parseFrom(data);
            log.info("KAFKA MESSAGE RECEIVED!");
            log.info("Patient ID: " + event.getPatientId());
            log.info("Patient Name: " + event.getName());
        } catch (Exception e) {
            log.error("Failed to parse Kafka message", e);
        }
    }
}
