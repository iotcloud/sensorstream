package cgl.sensorstream.storm.perf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Endpoint {
    private String type;
    private String url;
    private List<String> iotServers = new ArrayList<String>();

    private Map<String, String> properties = new HashMap<String, String>();

    public Endpoint(String type, String url) {
        this.type = type;
        this.url = url;
    }

    public void addIotServer(String server) {
        iotServers.add(server);
    }

    public String getType() {
        return type;
    }

    public String getUrl() {
        return url;
    }

    public List<String> getIotServers() {
        return iotServers;
    }

    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
