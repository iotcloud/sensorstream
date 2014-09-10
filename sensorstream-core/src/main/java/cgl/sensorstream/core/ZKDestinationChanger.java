package cgl.sensorstream.core;

import com.ss.commons.DestinationChangeListener;
import com.ss.commons.DestinationChanger;

public class ZKDestinationChanger implements DestinationChanger {
    private SensorListener listener;

    private DestinationChangeListener dstListener;

    private String sensor;

    private String channel;

    private String zkConnectionString;

    private String topologyName;

    private int totalTasks;

    private int taskIndex;

    private boolean bolt;

    private boolean distributed;

    public ZKDestinationChanger(String topologyName, String sensor, String channel, String zkConnectionString) {
        this(topologyName, sensor, channel, zkConnectionString, false, false);
    }

    public ZKDestinationChanger(String topologyName, String sensor, String channel, String zkConnectionString, boolean bolt, boolean distributed) {
        this.sensor = sensor;
        this.channel = channel;
        this.zkConnectionString = zkConnectionString;
        this.topologyName = topologyName;
        this.bolt = bolt;
        this.distributed = true;
    }

    @Override
    public void start() {
        listener = new SensorListener(topologyName, sensor, channel, zkConnectionString, dstListener, taskIndex, totalTasks, bolt, distributed);
        listener.start();
    }

    @Override
    public void stop() {
        listener.close();
    }

    @Override
    public void registerListener(DestinationChangeListener destinationChangeListener) {
        dstListener = destinationChangeListener;
    }

    @Override
    public void setTask(int taskIndex, int totalTasks) {
        this.taskIndex = taskIndex;
        this.totalTasks = totalTasks;
    }

    @Override
    public int getTaskIndex() {
        return taskIndex;
    }

    @Override
    public int getTotalTasks() {
        return totalTasks;
    }
}
