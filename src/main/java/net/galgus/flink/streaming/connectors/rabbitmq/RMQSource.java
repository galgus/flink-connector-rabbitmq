package net.galgus.flink.streaming.connectors.rabbitmq;

import com.rabbitmq.client.*;
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
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RMQSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, Long> implements ResultTypeQueryable<OUT> {
    private final static Logger log = LoggerFactory.getLogger(RMQSource.class);

    private final RMQConnectionConfig rmqConnectionConfig;
    protected final String queueName;
    private final boolean usesCorrelationId;
    protected DeserializationSchema<OUT> deserializationSchema;
    
    protected transient Connection connection;
    protected transient Channel channel;
    
    protected transient boolean autoAck;
    
    private transient volatile boolean running;
    
    private DeliverCallback deliverCallback;     
    
    private transient Object waitLock;
    
    public RMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, 
                     DeserializationSchema<OUT> deserializationSchema) {
        this(rmqConnectionConfig, queueName, false, deserializationSchema);
    }
    
    public RMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, boolean usesCorrelationId, 
                     DeserializationSchema<OUT> deserializationSchema) {
        super(String.class);
        
        this.rmqConnectionConfig = rmqConnectionConfig;
        this.queueName = queueName;
        this.usesCorrelationId = usesCorrelationId;
        this.deserializationSchema = deserializationSchema;
    }
    
    protected ConnectionFactory setupConnectionFactory() throws Exception {
        return rmqConnectionConfig.getConnectionFactory();
    }
    
    protected void setupQueue() throws IOException {
        channel.queueDeclare(queueName, true, false, false, null);
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

            setupQueue();
            
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
            throw new RuntimeException("Cannot create RMQ connection with " + queueName + " at " + rmqConnectionConfig.getHost(), e);
        }
        
        running = true;
    }

    @Override
    public void close() throws Exception {
        super.close();
        try {
            if (connection != null) connection.close();
        } catch (IOException e) {
            throw new RuntimeException("Error while closing RMQ connection with " + queueName + " at " + rmqConnectionConfig.getHost(), e);
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
        deliverCallback = (consumerTag, delivery) -> {
            boolean skip = false;
            OUT result = deserializationSchema.deserialize(delivery.getBody());
            
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
