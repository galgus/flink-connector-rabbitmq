package net.galgus.flink.streaming.connectors.rabbitmq.custom;

import com.rabbitmq.client.Delivery;

import java.io.Serializable;

@FunctionalInterface
public interface OnDeserialize extends Serializable {
    OnDeserialize DEFAULT = (consumerTag, delivery) -> delivery.getBody();

    byte[] onDeserialize(String consumerTag, Delivery delivery);
}
