package cgl.sensorstream.core;

public enum ChannelListenerState {
    INIT,
    WAITING_FOR_LEADER,
    LEADER,
    LEADER_LEFT
}
