package cgl.sensorstream.sensors;

public class SensorConfiguration {
    private int noSensors;

    private String baseSendQueueName;

    private String baseRecvQueueName;

    private int sendInterval;

    private String fileName;

    public SensorConfiguration(int noSensors, String baseSendQueueName, String baseRecvQueueName, int sendInterval, String fileName) {
        this.noSensors = noSensors;
        this.baseSendQueueName = baseSendQueueName;
        this.baseRecvQueueName = baseRecvQueueName;
        this.sendInterval = sendInterval;
        this.fileName = fileName;
    }

    public int getNoSensors() {
        return noSensors;
    }

    public String getBaseSendQueueName() {
        return baseSendQueueName;
    }

    public String getBaseRecvQueueName() {
        return baseRecvQueueName;
    }

    public int getSendInterval() {
        return sendInterval;
    }

    public String getFileName() {
        return fileName;
    }
}
