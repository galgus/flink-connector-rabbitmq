package net.galgus.flink.streaming.connectors.rabbitmq.common;

import com.rabbitmq.client.ConnectionFactory;
import net.galgus.flink.streaming.connectors.rabbitmq.custom.OnDeserialize;
import net.galgus.flink.streaming.connectors.rabbitmq.custom.SetupChannel;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class RMQConnectionConfig implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(RMQConnectionConfig.class);

    private String host;
    private Integer port;
    private String virtualHost;
    private String username;
    private String password;
    private String uri;
    
    private Integer networkRecoveryInterval;
    private Boolean automaticRecovery;
    private Boolean topologyRecovery;
    
    private Integer connectionTimeout;
    private Integer requestedChannelMax;
    private Integer requestedFrameMax;
    private Integer requestedHeartbeat;

    private OnDeserialize onDeserialize = OnDeserialize.DEFAULT;
    private SetupChannel setupChannel = SetupChannel.DEFAULT; 

    public RMQConnectionConfig(String host, Integer port, String virtualHost, String username, String password, 
                               Integer networkRecoveryInterval, Boolean automaticRecovery, Boolean topologyRecovery, 
                               Integer connectionTimeout, Integer requestedChannelMax, Integer requestedFrameMax, 
                               Integer requestedHeartbeat) {
        Preconditions.checkNotNull(host, "host can not be null");
        Preconditions.checkNotNull(port, "port can not be null");
        Preconditions.checkNotNull(virtualHost, "virtualHost can not be nul");
        Preconditions.checkNotNull(username, "username can not be null");
        Preconditions.checkNotNull(password, "password can not be null");
        
        this.host = host;
        this.port = port;
        this.virtualHost = virtualHost;
        this.username = username;
        this.password = password;
        
        this.networkRecoveryInterval = networkRecoveryInterval;
        this.automaticRecovery = automaticRecovery;
        this.topologyRecovery = topologyRecovery;
        this.connectionTimeout = connectionTimeout;
        this.requestedChannelMax = requestedChannelMax;
        this.requestedFrameMax = requestedFrameMax;
        this.requestedHeartbeat = requestedHeartbeat;
    }

    public RMQConnectionConfig(String uri, Integer networkRecoveryInterval, Boolean automaticRecovery, 
                               Boolean topologyRecovery, Integer connectionTimeout, Integer requestedChannelMax, 
                               Integer requestedFrameMax, Integer requestedHeartbeat) {
        Preconditions.checkNotNull(uri, "uri can not be null");
        this.uri = uri;
        
        this.networkRecoveryInterval = networkRecoveryInterval;
        this.automaticRecovery = automaticRecovery;
        this.topologyRecovery = topologyRecovery;
        this.connectionTimeout = connectionTimeout;
        this.requestedChannelMax = requestedChannelMax;
        this.requestedFrameMax = requestedFrameMax;
        this.requestedHeartbeat = requestedHeartbeat;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getUri() {
        return uri;
    }

    public Integer getNetworkRecoveryInterval() {
        return networkRecoveryInterval;
    }

    public Boolean getAutomaticRecovery() {
        return automaticRecovery;
    }

    public Boolean getTopologyRecovery() {
        return topologyRecovery;
    }

    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    public Integer getRequestedChannelMax() {
        return requestedChannelMax;
    }

    public Integer getRequestedFrameMax() {
        return requestedFrameMax;
    }

    public Integer getRequestedHeartbeat() {
        return requestedHeartbeat;
    }

    public OnDeserialize getOnDeserialize() {
        return onDeserialize;
    }

    public void useOnDeserialize(OnDeserialize onDeserialize) {
        this.onDeserialize = onDeserialize;
    }

    public SetupChannel getSetupChannel() {
        return setupChannel;
    }

    public void useSetupChannel(SetupChannel setupChannel) {
        this.setupChannel = setupChannel;
    }

    public ConnectionFactory getConnectionFactory() throws URISyntaxException, 
            NoSuchAlgorithmException, KeyManagementException {
        
        ConnectionFactory connectionFactory = new ConnectionFactory();
        if (uri != null && !uri.isEmpty()) {
            try {
                connectionFactory.setUri(uri);
            } catch (URISyntaxException e) {
                log.error("Failed to parse uri", e);
                throw e;
            } catch (KeyManagementException e) {
                log.error("Failed tyo setup ssl factory", e);
                throw e;
            } catch (NoSuchAlgorithmException e) {
                log.error("Failed to setup ssl factory", e);
                throw e;
            }
        } else {
            connectionFactory.setHost(host);
            connectionFactory.setPort(port);
            connectionFactory.setVirtualHost(virtualHost);
            connectionFactory.setUsername(username);
            connectionFactory.setPassword(password);
        }
        
        if (automaticRecovery != null) connectionFactory.setAutomaticRecoveryEnabled(automaticRecovery);
        
        if (connectionTimeout != null) connectionFactory.setConnectionTimeout(connectionTimeout);
        
        if (networkRecoveryInterval != null) connectionFactory.setRequestedHeartbeat(networkRecoveryInterval);
        
        if (requestedHeartbeat != null) connectionFactory.setRequestedHeartbeat(requestedHeartbeat);
        
        if (topologyRecovery != null) connectionFactory.setTopologyRecoveryEnabled(topologyRecovery);
        
        if (requestedChannelMax != null) connectionFactory.setRequestedChannelMax(requestedChannelMax);
        
        if (requestedFrameMax != null) connectionFactory.setRequestedFrameMax(requestedFrameMax);
        
        return connectionFactory;
    }
    
//    @Override
//    public String toString() {
//        StringBuilder stringBuilder = new StringBuilder();
//        stringBuilder
//                .append("host: " + host)
//                .append("port: " + port)
//                .append("virtualHost: " + virtualHost)
//                .append("username: " + )
//                .append("password: " + String.format("%*s" + password.length() +""), )
//    }
//    
    public static class Builder {
        private String host;
        private Integer port;
        private String virtualHost;
        private String username;
        private String password;
        
        private Integer networkRecoveryInterval;
        private Boolean automaticRecovery;
        private Boolean topologyRecovery;
        
        private Integer connectionTimeout;
        private Integer requestedChannelMax;
        private Integer requestedFrameMax;
        private Integer requestedHeartbeat;
        
        private String uri;

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        public Builder setUserName(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setUri(String uri) {
            this.uri = uri;
            return this;
        }

        public Builder setTopologyRecoveryEnabled(boolean topologyRecovery) {
            this.topologyRecovery = topologyRecovery;
            return this;
        }

        public Builder setRequestedHeartbeat(int requestedHeartbeat) {
            this.requestedHeartbeat = requestedHeartbeat;
            return this;
        }

        public Builder setRequestedFrameMax(int requestedFrameMax) {
            this.requestedFrameMax = requestedFrameMax;
            return this;
        }

        public Builder setRequestedChannelMax(int requestedChannelMax) {
            this.requestedChannelMax = requestedChannelMax;
            return this;
        }

        public Builder setNetworkRecoveryInterval(int networkRecoveryInterval) {
            this.networkRecoveryInterval = networkRecoveryInterval;
            return this;
        }

        public Builder setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder setAutomaticRecovery(boolean automaticRecovery) {
            this.automaticRecovery = automaticRecovery;
            return this;
        }

        public RMQConnectionConfig build(){
            if (this.uri != null) {
                return new RMQConnectionConfig(this.uri, this.networkRecoveryInterval,
                        this.automaticRecovery, this.topologyRecovery, this.connectionTimeout, this.requestedChannelMax,
                        this.requestedFrameMax, this.requestedHeartbeat);
            } else {
                return new RMQConnectionConfig(this.host, this.port, this.virtualHost, this.username, this.password,
                        this.networkRecoveryInterval, this.automaticRecovery, this.topologyRecovery,
                        this.connectionTimeout, this.requestedChannelMax, this.requestedFrameMax, this.requestedHeartbeat);
            }
        }
    }
    
}
