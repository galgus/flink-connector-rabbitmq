package net.galgus.flink.streaming.connectors.rabbitmq;

import com.rabbitmq.client.*;
import net.galgus.flink.streaming.connectors.rabbitmq.custom.OnDeserialize;
import net.galgus.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class RMQSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, Long> implements ResultTypeQueryable<OUT> {
    private final static Logger log = LoggerFactory.getLogger(RMQSource.class);

    private final RMQConnectionConfig rmqConnectionConfig;

    private final boolean usesCorrelationId;
    protected DeserializationSchema<OUT> deserializationSchema;
    
    protected transient Connection connection;
    protected transient Channel channel;
    
    protected String queueName;
    
    protected transient boolean autoAck;
    
    private transient volatile boolean running;
    
    private transient Object waitLock;

    public RMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, boolean usesCorrelationId,
                     DeserializationSchema<OUT> deserializationSchema, OnDeserialize onDeserialize) {
        super(String.class);
        
        this.queueName = queueName;
        this.rmqConnectionConfig = rmqConnectionConfig;
        this.usesCorrelationId = usesCorrelationId;
        this.deserializationSchema = deserializationSchema;
    }
    
    public RMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, 
                     DeserializationSchema deserializationSchema, OnDeserialize onDeserialize) {
        this(rmqConnectionConfig, queueName,false, deserializationSchema, onDeserialize);
    }

    public RMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName,
                     DeserializationSchema deserializationSchema) {
        this(rmqConnectionConfig, queueName, deserializationSchema,null);
    }
   
    
    protected ConnectionFactory setupConnectionFactory() throws Exception {
        return rmqConnectionConfig.getConnectionFactory();
    }
    
    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        
        waitLock = new Object();
        
        ConnectionFactory connectionFactory = setupConnectionFactory();
        
        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            
            if (channel == null) throw new RuntimeException("None of RabbitMQ channels are available");
            
            queueName = rmqConnectionConfig.getSetupChannel().setupChannel(channel, queueName);
            
            RuntimeContext runtimeContext = getRuntimeContext();
            if (runtimeContext instanceof StreamingRuntimeContext 
                    && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled()) {
                autoAck = false;
                // enables transaction mode
                channel.txSelect();
            } else {
              autoAck = true;  
            }
            
            log.debug("Starting RabbitMQ source with autoAck status: " + autoAck);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create RMQ connection at " + rmqConnectionConfig.getHost(), e);
        }
        
        running = true;
    }

    @Override
    public void close() throws Exception {
        super.close();
        try {
            if (connection != null) connection.close();
        } catch (IOException e) {
            throw new RuntimeException("Error while closing RMQ connection at " + rmqConnectionConfig.getHost(), e);
        }
        
        synchronized (waitLock) {
            waitLock.notify();
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() { return deserializationSchema.getProducedType(); }

    @Override
    protected void acknowledgeSessionIDs(List<Long> list) {
        try {
            for (long id : sessionIds) {
                channel.basicAck(id, false);
            }
            channel.txCommit();
        } catch (IOException e) {
            throw new RuntimeException("Messages could not be acknowledged during checkpoint creation", e);
        }
    }
    
    @Override
    public void run(SourceContext<OUT> sourceContext) throws Exception {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            boolean skip = false;
            
            byte[] bytes = rmqConnectionConfig.getOnDeserialize().onDeserialize(consumerTag, delivery);
            
            OUT result = deserializationSchema.deserialize(bytes);
            Envelope envelope = delivery.getEnvelope();
            
            if (deserializationSchema.isEndOfStream(result)) {
                running = false;
            }
            
            if (running) {
                if (!autoAck) {
                    final long deliveryTag = envelope.getDeliveryTag();
                    if (usesCorrelationId) {
                        final String correlationId = delivery.getProperties().getCorrelationId();
                        Preconditions.checkNotNull(correlationId, "RabbitMQ source was instantiated " +
                                "with usesCorrelationId set to true, but a message was received with " +
                                "correlation id set to null!");

                        // we have already processed this message
                        if (!addId(correlationId)) skip = true;
                    }
                    if (!skip) sessionIds.add(deliveryTag);
                }
                if (!skip) sourceContext.collect(result);
            }
        };

        channel.basicConsume(queueName, autoAck, deliverCallback, id -> {});
        
        log.info("RabbitMQ connection established successfully");
        
        while (running) {
            synchronized (waitLock) {
                waitLock.wait(100L); 
            }
        }
    }

    @Override
    public void cancel() {
        log.info("Cancelling RabbitMQ source");
        running = false;
        try {
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
