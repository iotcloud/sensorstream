package cgl.sensorstream.core;


import cgl.iotcloud.core.api.thrift.TChannel;
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

import java.util.List;

public class ChannelListener {
    private Logger LOG = LoggerFactory.getLogger(ChannelListener.class);

    private CuratorFramework client = null;
    private PathChildrenCache cache = null;

    private String sensorPath = null;
    private String channel = null;

    public ChannelListener(String sensorPath, String channel, String connectionString) {
        try {
            this.sensorPath = sensorPath;
            this.channel = channel;

            client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3));
            client.start();

            cache = new PathChildrenCache(client, sensorPath, true);
            cache.start();
        } catch (Exception e) {
            String msg = "Failed to create the listener for ZK path " + sensorPath;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

    }

    private static void addListener(PathChildrenCache cache) {
        // a PathChildrenCacheListener is optional. Here, it's used just to log changes
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED: {
                        System.out.println("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }

                    case CHILD_UPDATED: {
                        System.out.println("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }

                    case CHILD_REMOVED: {
                        System.out.println("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    public void start() {
        if (cache.getCurrentData().size() == 0) {

        } else {
            for (ChildData data : cache.getCurrentData()) {
                String path = data.getPath();


            }
        }
    }

    private List<TChannel> getChannels(String sensorId) {
        return ;
    }

    public void close() {
        CloseableUtils.closeQuietly(cache);
        CloseableUtils.closeQuietly(client);
    }
}
