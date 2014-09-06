package cgl.sensorstream.core;

import cgl.iotcloud.core.api.thrift.TChannel;
import cgl.iotcloud.core.utils.SerializationUtils;
import com.ss.commons.DestinationChangeListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ChannelListener {
    private static Logger LOG = LoggerFactory.getLogger(ChannelListener.class);

    private CuratorFramework client = null;

    private String channelPath = null;

    private LeaderSelector leaderSelector;

    private final Lock lock = new ReentrantLock();

    private final Condition condition = lock.newCondition();

    private DestinationChangeListener dstListener;

    private ChannelListenerState state = ChannelListenerState.WAITING_FOR_LEADER;

    private ChannelsState channelsState;

    private boolean bolt = false;

    private TChannel channel;

    public ChannelListener(String channelPath, String connectionString,
                           DestinationChangeListener dstListener, ChannelsState channelsState) {
        this(channelPath, connectionString, dstListener, channelsState, false);
    }

    public ChannelListener(String channelPath, String connectionString,
                           DestinationChangeListener dstListener, ChannelsState channelsState, boolean bolt) {
        try {
            this.channelPath = channelPath;
            this.dstListener = dstListener;
            this.channelsState = channelsState;
            this.bolt = bolt;
            client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3));
            client.start();
        } catch (Exception e) {
            String msg = "Failed to create the listener for ZK path " + channelPath;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }

    public void start() {
        if (!bolt) {
            leaderSelector = new LeaderSelector(client, channelPath, new ChannelLeaderSelector());
            leaderSelector.start();
            leaderSelector.autoRequeue();
            state = ChannelListenerState.WAITING_FOR_LEADER;
        } else {
            byte data[];
            try {
                data = client.getData().forPath(channelPath);
                TChannel channel = new TChannel();
                SerializationUtils.createThriftFromBytes(data, channel);
                if (dstListener != null) {
                    dstListener.addDestination(channel.getSensorId(), Utils.convertChannelToDestination(channel));
                }
            } catch (Exception e) {
                LOG.error("Failed to start a destination listener", e);
            }
        }
    }

    public void stop() {
        lock.lock();
        try {
            // leaderSelector.close();
            if (!bolt) {
                condition.signal();
            } else {
                if (dstListener != null && channel != null) {
                    dstListener.removeDestination(channel.getName());
                }
                channelsState.removeLeader();
            }
        } catch (Exception e) {
            LOG.error("Failed to get data", e);
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        CloseableUtils.closeQuietly(leaderSelector);
        CloseableUtils.closeQuietly(client);
    }

    private class ChannelLeaderSelector extends LeaderSelectorListenerAdapter {
        @Override
        public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
            LOG.info(channelPath + " is the new leader.");
            lock.lock();
            try {
                if (channelsState.addLeader()) {
                    byte data[] = curatorFramework.getData().forPath(channelPath);
                    channel = new TChannel();
                    SerializationUtils.createThriftFromBytes(data, channel);
                    if (dstListener != null) {
                        dstListener.addDestination(channel.getSensorId(), Utils.convertChannelToDestination(channel));
                    }

                    state = ChannelListenerState.LEADER;
                    condition.await();
                    if (dstListener != null) {
                        dstListener.removeDestination(channel.getName());
                    }
                    channelsState.removeLeader();
                    state = ChannelListenerState.LEADER_LEFT;
                }
            } catch (InterruptedException e) {
                LOG.info(channelPath + " leader was interrupted.");
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
                LOG.info(channelPath + " leader relinquishing leadership.\n");
            }
        }
    }

    public ChannelListenerState getState() {
        return state;
    }
}
