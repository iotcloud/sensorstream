package cgl.sensorstream.core;

import cgl.iotcloud.core.api.thrift.TChannel;
import cgl.iotcloud.core.utils.SerializationUtils;
import com.google.common.base.Joiner;
import com.ss.commons.DestinationChangeListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GroupedChannelListener {
    private static Logger LOG = LoggerFactory.getLogger(GroupedChannelListener.class);

    private CuratorFramework client = null;

    private LeaderSelector leaderSelector;

    private final Lock lock = new ReentrantLock();

    private final Condition condition = lock.newCondition();

    private DestinationChangeListener dstListener;

    private String channelLeaderPath;

    private String channelPath;

    private ChannelListenerState state = ChannelListenerState.INIT;

    private ChannelsState channelsState;

    private boolean bolt = false;

    private TChannel channel;

    private String groupName;

    private boolean distributed;

    public GroupedChannelListener(CuratorFramework client, String channelPath, String parent, String topology, String site, String sensor,
                                  String channel, String connectionString,
                                  DestinationChangeListener dstListener, ChannelsState channelsState, boolean bolt, boolean distributed) {
        try {
            this.groupName = Utils.getGroupName(topology, site, sensor, channel);
            this.channelPath = channelPath;
            this.channelLeaderPath = Joiner.on("/").join(parent, topology, site, sensor, channel);
            this.dstListener = dstListener;
            this.bolt = bolt;
            this.distributed = distributed;
            this.channelsState = channelsState;
            this.client = client;
        } catch (Exception e) {
            String msg = "Failed to create the listener for ZK path " + channelPath;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }

    public GroupedChannelListener(CuratorFramework client, String channelPath, String parent, String topology, String site, String sensor,
                                  String channel, String connectionString,
                                  DestinationChangeListener dstListener, ChannelsState channelsState) {
        this(client, channelPath, parent, topology, site, sensor, channel, connectionString, dstListener, channelsState, false, false);
    }

    public void start() {
        try {
            if (!bolt && !distributed) {
                if (client.checkExists().forPath(channelLeaderPath) == null) {
                    client.create().creatingParentsIfNeeded().forPath(channelLeaderPath);
                }
                leaderSelector = new LeaderSelector(client, channelLeaderPath, new ChannelLeaderSelector());
                leaderSelector.start();
                leaderSelector.autoRequeue();
                state = ChannelListenerState.WAITING_FOR_LEADER;
            } else {
                byte data[] = client.getData().forPath(channelPath);
                TChannel channel = new TChannel();
                SerializationUtils.createThriftFromBytes(data, channel);
                if (dstListener != null) {
                    dstListener.addDestination(groupName, Utils.convertChannelToDestination(channel));
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to access zookeeper", e);
        }

    }

    public void addPath(String path) {
        if (dstListener != null) {
            dstListener.addPathToDestination(groupName, path);
        }
    }

    public void stop() {
        LOG.info("Stopping channel listener {}", channelPath);
        lock.lock();
        try {
            // leaderSelector.close();
            if (!bolt) {
                condition.signal();
            } else {
                if (dstListener != null && channel != null) {
                    dstListener.removeDestination(groupName);
                }
                channelsState.removeLeader();
            }
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        CloseableUtils.closeQuietly(leaderSelector);
    }

    private class ChannelLeaderSelector extends LeaderSelectorListenerAdapter {
        @Override
        public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
            LOG.info(channelLeaderPath + " is the new leader.");
            lock.lock();
            try {
                if (channelsState.addLeader()) {
                    byte data[] = curatorFramework.getData().forPath(channelPath);
                    channel = new TChannel();
                    SerializationUtils.createThriftFromBytes(data, channel);
                    if (dstListener != null) {
                        dstListener.addDestination(groupName, Utils.convertChannelToDestination(channel));
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
                LOG.info(channelLeaderPath + " leader was interrupted.");
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
                LOG.info(channelLeaderPath + " leader relinquishing leadership.\n");
            }
        }
    }

    public ChannelListenerState getState() {
        return state;
    }
}
