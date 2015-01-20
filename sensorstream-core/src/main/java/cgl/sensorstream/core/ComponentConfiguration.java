package cgl.sensorstream.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComponentConfiguration implements Serializable {
    private TopologyConfiguration topologyConfiguration;

    private int queueSize = 64;

    private String messageBuilder;

    private List<String> fields;

    private String sensor;

    private String channel;

    private Map<String, String> properties = new HashMap<String, String>();

    private String stream;

    public ComponentConfiguration(TopologyConfiguration topologyConfiguration,
                                  String sensor, String channel,
                                  List<String> fields, String messageBuilder) {
        this.sensor = sensor;
        this.channel = channel;
        this.fields = fields;
        this.messageBuilder = messageBuilder;
        this.topologyConfiguration = topologyConfiguration;
    }

    public ComponentConfiguration() {
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public TopologyConfiguration getTopologyConfiguration() {
        return topologyConfiguration;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public String getMessageBuilder() {
        return messageBuilder;
    }

    public List<String> getFields() {
        return fields;
    }

    public String getSensor() {
        return sensor;
    }

    public String getChannel() {
        return channel;
    }

    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setTopologyConfiguration(TopologyConfiguration topologyConfiguration) {
        this.topologyConfiguration = topologyConfiguration;
    }

    public void setMessageBuilder(String messageBuilder) {
        this.messageBuilder = messageBuilder;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public void setSensor(String sensor) {
        this.sensor = sensor;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }
}
