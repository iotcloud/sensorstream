package cgl.sensorstream.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComponentConfiguration {
    private TopologyConfiguration topologyConfiguration;

    private int queueSize = 64;

    private String messageBuilder;

    private List<String> fields;

    private String sensor;

    private String channel;

    private Map<String, String> properties = new HashMap<String, String>();

    public ComponentConfiguration(TopologyConfiguration topologyConfiguration,
                                  String sensor, String channel,
                                  List<String> fields, String messageBuilder) {
        this.sensor = sensor;
        this.channel = channel;
        this.fields = fields;
        this.messageBuilder = messageBuilder;
        this.topologyConfiguration = topologyConfiguration;
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
}
