package cgl.sensorstream.core;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ChannelListener {
    private static Logger LOG = LoggerFactory.getLogger(ChannelListener.class);

    private CuratorFramework client = null;

    private String channelPath = null;

    private LeaderSelector leaderSelector;

    public ChannelListener(String channelPath, String connectionString) {
        try {
            this.channelPath = channelPath;

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
    }

    public void close() {
        CloseableUtils.closeQuietly(leaderSelector);
        CloseableUtils.closeQuietly(client);
    }

    private class ChannelLeaderSelector extends LeaderSelectorListenerAdapter {
        @Override
        public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
            // we are now the leader. This method should not return until we want to relinquish leadership
            final int waitSeconds = (int)(5 * Math.random()) + 1;

            LOG.info(channelPath + " os the new leader.");
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
            } catch (InterruptedException e) {
                LOG.info(channelPath + " leader was interrupted.");
                Thread.currentThread().interrupt();
            } finally {
                LOG.info(channelPath + " leader relinquishing leadership.\n");
            }
        }
    }
}
