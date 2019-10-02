package net.galgus.flink.streaming.connectors.rabbitmq.custom;

import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.io.Serializable;

@FunctionalInterface
public interface SetupChannel extends Serializable {
    SetupChannel DEFAULT = (channel, queue) -> {
        channel.queueDeclare(queue, false, false, false, null);
        return queue;
    };

    String setupChannel(Channel channel, String queue) throws IOException;
}
