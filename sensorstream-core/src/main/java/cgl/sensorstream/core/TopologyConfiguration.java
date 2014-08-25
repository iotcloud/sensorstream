package cgl.sensorstream.core;

public class TopologyConfiguration {
    private String zkConnectionString;

    private String topologyName;

    private String zkRoot;

    public TopologyConfiguration(String topologyName, String zkConnectionString, String zkRoot) {
        this.topologyName = topologyName;
        this.zkConnectionString = zkConnectionString;
        this.zkRoot = zkRoot;
    }

    public String getZkConnectionString() {
        return zkConnectionString;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public String getZkRoot() {
        return zkRoot;
    }
}
