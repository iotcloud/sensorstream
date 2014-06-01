package cgl.sensorstream.storm.perf;

import java.io.Serializable;
import java.util.List;

public class TopologyConfiguration implements Serializable {
    private int noWorkers = 4;

    private String topologyName = "perf";

    private List<String> ip;

    private int noQueues;

    private String recevBaseQueueName;

    private String sendBaseQueueName;

    public TopologyConfiguration(List<String> ip, int noQueues, String baseQueueName, String sendBaseQueueName) {
        this.ip = ip;
        this.noQueues = noQueues;
        this.recevBaseQueueName = baseQueueName;
        this.sendBaseQueueName = sendBaseQueueName;
    }

    public int getNoWorkers() {
        return noWorkers;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public List<String> getIp() {
        return ip;
    }

    public int getNoQueues() {
        return noQueues;
    }

    public String getRecevBaseQueueName() {
        return recevBaseQueueName;
    }

    public void setNoWorkers(int noWorkers) {
        this.noWorkers = noWorkers;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public String getSendBaseQueueName() {
        return sendBaseQueueName;
    }
}
