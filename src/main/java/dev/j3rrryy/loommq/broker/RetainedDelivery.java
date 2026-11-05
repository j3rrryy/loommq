package dev.j3rrryy.loommq.broker;

public record RetainedDelivery(String topic, RetainedMessage message) {

}
