package cgl.sensorstream.core;

import com.ss.commons.DestinationChangeListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SensorListener {
    private static Logger LOG = LoggerFactory.getLogger(SensorListener.class);

    private CuratorFramework client = null;
    private PathChildrenCache cache = null;

    private String channel = null;

    private String connectionString = null;

    private Map<String, ChannelListener> channelListeners = new HashMap<String, ChannelListener>();

    private DestinationChangeListener dstListener;

    public SensorListener(String sensorPath, String channel, String connectionString, DestinationChangeListener listener) {
        try {
            this.channel = channel;
            this.connectionString = connectionString;
            this.dstListener = listener;

            client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3));
            client.start();

            cache = new PathChildrenCache(client, sensorPath, true);
            cache.start();
            addListener(cache);
        } catch (Exception e) {
            String msg = "Failed to create the listener for ZK path " + sensorPath;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }

    private void addListener(PathChildrenCache cache) {
        // a PathChildrenCacheListener is optional. Here, it's used just to log changes
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED: {
                        if (event.getData().getPath().equals(channel)) {
                            LOG.info("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                            startListener(event.getData().getPath());
                        }
                        break;
                    } case CHILD_UPDATED: {
                        if (event.getData().getPath().equals(channel)) {
                            LOG.info("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                            stopListener();
                            startListener(event.getData().getPath());
                        }
                        LOG.info("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    } case CHILD_REMOVED: {
                        LOG.info("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        if (event.getData().getPath().equals(channel)) {
                            stopListener();
                        }
                        break;
                    }
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    private void stopListener() {
        ChannelListener listener = channelListeners.get(channel);
        listener.stop();
    }

    public void start() {
        if (cache.getCurrentData().size() != 0) {
            for (ChildData data : cache.getCurrentData()) {
                String path = data.getPath();
                startListener(path);
            }
        }
    }

    private void startListener(String path) {
        String channelPath = path + "/" + channel;
        try {
            if (client.checkExists().forPath(channelPath) != null) {
                ChannelListener channelListener = new ChannelListener(channelPath, connectionString, dstListener);
                channelListeners.put(path, channelListener);
            }
        } catch (Exception e) {
            String msg = "Failed to get the information about channel " + channelPath;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }

    public void close() {
        CloseableUtils.closeQuietly(cache);
        CloseableUtils.closeQuietly(client);
    }
}
