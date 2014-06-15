package cgl.sensorstream.storm.perf;

import java.io.Serializable;
import java.util.*;

public class TopologyConfiguration implements Serializable {
    private int noWorkers = 4;
    private String name = "perf";
    private Map<String, Endpoint> endpoints = new HashMap<String, Endpoint>();
    private String recv;
    private String send;
    private Map<String, String> properties = new HashMap<String, String>();

    public int getNoWorkers() {
        return noWorkers;
    }

    public String getName() {
        return name;
    }

    public String getRecv() {
        return recv;
    }

    public void setNoWorkers(int noWorkers) {
        this.noWorkers = noWorkers;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSend() {
        return send;
    }

    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public List<Endpoint> getEndpoints() {
        return new ArrayList<Endpoint>(endpoints.values());
    }

    public void setSend(String send) {
        this.send = send;
    }

    public void setRecv(String recv) {
        this.recv = recv;
    }

    public void addEndpoint(String name, Endpoint endpoint) {
        endpoints.put(name, endpoint);
    }
}
