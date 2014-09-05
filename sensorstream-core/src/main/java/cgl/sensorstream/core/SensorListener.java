package cgl.sensorstream.core;

import backtype.storm.daemon.task__init;
import cgl.iotcloud.core.api.thrift.TChannel;
import cgl.iotcloud.core.api.thrift.TSensor;
import cgl.iotcloud.core.api.thrift.TSensorState;
import cgl.iotcloud.core.utils.SerializationUtils;
import com.google.common.base.Joiner;
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

import java.util.*;

public class SensorListener {
    private static Logger LOG = LoggerFactory.getLogger(SensorListener.class);

    private CuratorFramework client = null;
    private PathChildrenCache cache = null;

    private String channel = null;

    private String connectionString = null;

    private Map<String, ChannelListener> singleChannelListeners = new HashMap<String, ChannelListener>();

    private Map<String, GroupedChannelListener> groupedChannelListeners = new HashMap<String, GroupedChannelListener>();

    private Map<String, List<String>> sensorsForGroup = new HashMap<String, List<String>>();

    private DestinationChangeListener dstListener;

    private String root = "/iot/sensors";

    private boolean run = true;

    private Thread updater;

    private String topologyName;

    private String parent = "/iot";

    private String sensor;

    private int taskIndex;

    private int totalTasks;

    public SensorListener(String topologyName, String sensor, String channel, String connectionString,
                          DestinationChangeListener listener, int taskIndex, int totalTasks) {
        try {
            this.topologyName = topologyName;
            this.channel = channel;
            this.connectionString = connectionString;
            this.dstListener = listener;
            this.sensor = sensor;
            this.taskIndex = taskIndex;
            this.totalTasks = totalTasks;

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

    public void start() {
        if (cache.getCurrentData().size() != 0) {
            for (ChildData data : cache.getCurrentData()) {
                String path = data.getPath();
                startListenerForChannel(client, path);
            }
        }
    }

    public void close() {
        run = false;
        // wait until updater thread finishes
        try {
            updater.join();
        } catch (InterruptedException ignore) {
        }
        CloseableUtils.closeQuietly(cache);
        CloseableUtils.closeQuietly(client);
    }

    private void addListener(PathChildrenCache cache) {
        // a PathChildrenCacheListener is optional. Here, it's used just to log changes
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                int noOfChildren = client.getChildren().forPath(root + "/" + sensor).size();
                int possibleListeners = (int) Math.ceil(noOfChildren / totalTasks);
                switch (event.getType()) {
                    case CHILD_ADDED: {
                        LOG.info("Node added: {} for listening on channel {}", ZKPaths.getNodeFromPath(event.getData().getPath()), channel);
                        startListenerForChannel(client, event.getData().getPath());
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
        updater = new Thread(new UpdateWorker());
        updater.start();
    }

    private void updateChannelListener(PathChildrenCacheEvent event) throws TException {
        byte []data = event.getData().getData();
        TSensor sensor = new TSensor();
        SerializationUtils.createThriftFromBytes(data, sensor);
        if (sensor.getState() == TSensorState.UN_DEPLOY) {
            stopListener(event.getData().getPath());
        } else if (sensor.getState() == TSensorState.DEPLOY) {
            startListenerForChannel(client, event.getData().getPath());
        }
    }

    private void stopListener(String path) {
        String sensorId = Utils.getSensorIdFromPath(path);
        ChannelListener listener = singleChannelListeners.remove(sensorId);
        if (listener != null) {
            listener.stop();
        }
        // remove the sensor from the groups if they are present
        String removeGroup = null;
        for (Map.Entry<String, List<String>> e : sensorsForGroup.entrySet()) {
            Iterator<String> it = e.getValue().iterator();
            while (it.hasNext()) {
                String id = it.next();
                if (id.equals(sensorId)) {
                    it.remove();
                }
            }
            if (e.getValue().size() == 0) {
                removeGroup = e.getKey();
            }
        }

        if (removeGroup != null) {
            GroupedChannelListener groupedChannelListener = groupedChannelListeners.get(removeGroup);
            if (groupedChannelListener != null) {
                groupedChannelListener.stop();
            }

            sensorsForGroup.remove(removeGroup);
        }
    }

    private void startListenerForChannel(CuratorFramework client, String path) {
        String sensorId = Utils.getSensorIdFromPath(path);
        String channelPath = path + "/" + channel;

        try {
            TSensor sensor = new TSensor();
            byte []sensorData = client.getData().forPath(path);
            SerializationUtils.createThriftFromBytes(sensorData, sensor);

            if (client.checkExists().forPath(channelPath) != null) {
                byte []channelData = client.getData().forPath(channelPath);
                TChannel tChannel = new TChannel();
                SerializationUtils.createThriftFromBytes(channelData, tChannel);

                if (!tChannel.isGrouped()) {
                    if (sensor.getState() != TSensorState.UN_DEPLOY) {
                        LOG.info("Starting single listener on channel path {} for selecting the leader", channelPath);
                        ChannelListener channelListener = new ChannelListener(channelPath, connectionString, dstListener);
                        channelListener.start();
                        singleChannelListeners.put(sensorId, channelListener);
                    }
                } else {
                    if (sensor.getState() != TSensorState.UN_DEPLOY) {
                        String groupName = getGroupName(topologyName, tChannel.getSite(), tChannel.getSensor(), tChannel.getName());

                        // check weather we have a group
                        if (groupedChannelListeners.containsKey(groupName)) {
                            List<String> sensorIdsForGroup = this.sensorsForGroup.get(groupName);
                            sensorIdsForGroup.add(groupName);
                        } else {
                            LOG.info("Starting group listener on channel path {} for selecting the leader", channelPath);
                            GroupedChannelListener groupedChannelListener = new GroupedChannelListener(channelPath, parent, topologyName, tChannel.getSite(), tChannel.getSensor(), tChannel.getName(), connectionString, dstListener);
                            groupedChannelListener.start();
                            List<String> sensorIdsForGroup = new ArrayList<String>();
                            sensorIdsForGroup.add(tChannel.getSensorId());
                            sensorsForGroup.put(groupName, sensorIdsForGroup);
                            groupedChannelListeners.put(groupName, groupedChannelListener);
                        }
                    }
                }
            }
        } catch (Exception e) {
            String msg = "Failed to get the information about channel " + channelPath;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }

    private class UpdateWorker implements Runnable {
        @Override
        public void run() {
            while(run) {
                if (cache.getCurrentData().size() != 0) {
                    for (ChildData data : cache.getCurrentData()) {
                        String path = data.getPath();
                        String sensorId = Utils.getSensorIdFromPath(path);
                        if (!singleChannelListeners.containsKey(sensorId)) {
                            startListenerForChannel(client, path);
                        }
                    }
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private static String getGroupName(String topology, String site, String sensor, String channel) {
        return Joiner.on(".").join(topology, site, sensor, channel);
    }
}
