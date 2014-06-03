package cgl.sensorstream.storm.perf;

import java.io.Serializable;

public class BoltMessage implements Serializable {
    private String queue;

    private Object content;

    public BoltMessage(String queue, Object content) {
        this.queue = queue;
        this.content = content;
    }

    public String getQueue() {
        return queue;
    }

    public Object getContent() {
        return content;
    }
}
