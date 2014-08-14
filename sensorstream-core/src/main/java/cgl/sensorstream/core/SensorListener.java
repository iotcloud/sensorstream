package cgl.sensorstream.core;

import cgl.iotcloud.core.api.thrift.TSensor;
import cgl.iotcloud.core.api.thrift.TSensorState;
import cgl.iotcloud.core.utils.SerializationUtils;
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
import org.apache.thrift.TException;
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

    private String root = "/iot/sensors";

    public SensorListener(String sensor, String channel, String connectionString, DestinationChangeListener listener) {
        try {
            this.channel = channel;
            this.connectionString = connectionString;
            this.dstListener = listener;

            client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3));
            client.start();

            cache = new PathChildrenCache(client, root + "/" + sensor, true);
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            addListener(cache);
        } catch (Exception e) {
            String msg = "Failed to create the listener for ZK path " + sensor;
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
                        LOG.info("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        startListener(client, event.getData().getPath());
                        break;
                    } case CHILD_UPDATED: {
                        LOG.info("Node updated: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        updateChannelListener(event);
                        break;
                    } case CHILD_REMOVED: {
                        LOG.info("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        stopListener(event.getData().getPath());
                        break;
                    }
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    private void updateChannelListener(PathChildrenCacheEvent event) throws TException {
        byte []data = event.getData().getData();
        TSensor sensor = new TSensor();
        SerializationUtils.createThriftFromBytes(data, sensor);
        if (sensor.getState() == TSensorState.UN_DEPLOY) {
            stopListener(event.getData().getPath());
        }
    }

    private void stopListener(String path) {
        String sensorId = getSensorIdFromPath(path);

        ChannelListener listener = channelListeners.remove(sensorId);
        if (listener != null) {
            LOG.info("Stopping the leader selector for sensorId {}", sensorId);
            listener.stop();
        }
    }

    public void start() {
        if (cache.getCurrentData().size() != 0) {
            for (ChildData data : cache.getCurrentData()) {
                String path = data.getPath();
                String sensorId = getSensorIdFromPath(path);
                LOG.info("Starting the leader selector for sensorId {}", sensorId);
                startListener(client, path);
            }
        }
    }

    private void startListener(CuratorFramework client, String path) {
        String sensorId = getSensorIdFromPath(path);
        String channelPath = path + "/" + channel;
        try {
            if (client.checkExists().forPath(channelPath) != null) {
                byte []data = client.getData().forPath(path);
                TSensor sensor = new TSensor();
                SerializationUtils.createThriftFromBytes(data, sensor);
                if (sensor.getState() != null && sensor.getState() != TSensorState.UN_DEPLOY) {
                    ChannelListener channelListener = new ChannelListener(channelPath, connectionString, dstListener);
                    channelListener.start();
                    channelListeners.put(sensorId, channelListener);
                }
            }
        } catch (Exception e) {
            String msg = "Failed to get the information about channel " + channelPath;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }

    private String getSensorIdFromPath(String path) {
        if (path.contains("/")) {
            int index = path.lastIndexOf("/");
            if (index != path.length() - 1) {
                return path.substring(index + 1);
            } else {
                return null;
            }
        }
        return null;
    }

    public void close() {
        CloseableUtils.closeQuietly(cache);
        CloseableUtils.closeQuietly(client);
    }
}
