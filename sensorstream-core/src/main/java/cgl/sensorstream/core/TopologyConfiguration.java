package cgl.sensorstream.core;

import java.io.Serializable;

public class TopologyConfiguration implements Serializable  {
    private String zkConnectionString;

    private String topologyName;

    private String zkRoot;

    public TopologyConfiguration() {
    }

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

    public void setZkConnectionString(String zkConnectionString) {
        this.zkConnectionString = zkConnectionString;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public void setZkRoot(String zkRoot) {
        this.zkRoot = zkRoot;
    }
}
