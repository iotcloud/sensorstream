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

    public ChannelListener(String channelPath, String connectionString, DestinationChangeListener dstListener) {
        try {
            this.channelPath = channelPath;
            this.dstListener = dstListener;
            client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3));
            client.start();
        } catch (Exception e) {
            String msg = "Failed to create the listener for ZK path " + channelPath;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }

    public void start() {
        leaderSelector = new LeaderSelector(client, channelPath, new ChannelLeaderSelector());
        leaderSelector.start();
        leaderSelector.autoRequeue();
    }

    public void stop() {
        lock.lock();
        try {
            leaderSelector.close();
            condition.notify();
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
                byte data[] = curatorFramework.getData().forPath(channelPath);
                TChannel channel = new TChannel();
                SerializationUtils.createThriftFromBytes(data, channel);
                if (dstListener != null) {
                    dstListener.addDestination(channel.getName(), Utils.convertChannelToDestination(channel));
                }
                condition.await();
                if (dstListener != null) {
                    dstListener.removeDestination(channel.getName());
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
}
