package cgl.sensorstream.core;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ChannelsState {
    int possibleChannels = 0;

    int currentChannels = 0;

    int currentLeaders = 0;

    Lock lock = new ReentrantLock();

    public boolean addLeader() {
        lock.lock();
        try {
            if (possibleChannels > currentLeaders) {
                currentLeaders++;
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    public void addChannel(int totalTasks) {
        lock.lock();
        try {
            currentChannels++;
            possibleChannels = (int) Math.ceil(((double)currentChannels) / totalTasks);
        } finally {
            lock.unlock();
        }
    }

    public void removeChannel(int totalTasks) {
        lock.lock();
        try {
            currentChannels--;
            possibleChannels = (int) Math.ceil(((double)currentChannels) / totalTasks);
        } finally {
            lock.unlock();
        }
    }

    public void removeLeader() {
        lock.lock();
        try {
            currentLeaders--;
        } finally {
            lock.unlock();
        }
    }
}
