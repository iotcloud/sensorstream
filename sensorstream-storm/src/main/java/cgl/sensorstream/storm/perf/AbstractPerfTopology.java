package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import org.ho.yaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.*;

public abstract class AbstractPerfTopology {
    public static TopologyConfiguration parseArgs(String name, Map<String, String> properties) throws FileNotFoundException {
        TopologyConfiguration configuration = new TopologyConfiguration();

        Map conf = findAndReadConfigFile(name);
        Map eprs = (Map) conf.get("endpoints");
        String rQueue = (String) conf.get("recv");
        String sQueue = (String) conf.get("send");
        String tpName = (String) conf.get("name");
        int parelism = (Integer) conf.get("parallel");

        configuration.setRecv(rQueue);
        configuration.setSend(sQueue);
        configuration.setName(tpName);
        configuration.setParallism(parelism);
        if (properties != null) {
            for (Map.Entry<String, String> e : properties.entrySet()) {
                configuration.addProperty(e.getKey(), (String) conf.get(e.getKey()));
            }
        }

        for (Object e : eprs.entrySet()) {
            if (e instanceof Map.Entry) {
                String epr = (String) ((Map.Entry) e).getKey();
                if (((Map.Entry) e).getValue() instanceof Map) {
                    Map tp = (Map) ((Map.Entry) e).getValue();
                    String url = (String) tp.get("url");
                    String type = (String) tp.get("type");
                    Object iotServers = tp.get("iot_servers");
                    Endpoint endpoint = new Endpoint(type, url);
                    if (iotServers instanceof List) {
                        for (Object o : (List)iotServers) {
                            endpoint.addIotServer(o.toString());
                        }
                    }

                    for (Object o : tp.entrySet()) {
                        if (o instanceof Map.Entry) {
                            endpoint.addProperty(((Map.Entry) o).getKey().toString(), ((Map.Entry) o).getValue().toString());
                        }
                    }

                    configuration.addEndpoint(epr, endpoint);
                }
            }
        }
        return configuration;
    }

    public static Map findAndReadConfigFile(String file) throws FileNotFoundException {
        return (Map) Yaml.load(new InputStreamReader(new FileInputStream(file)));
    }

    public static boolean isLocal(String []args) {
        for (String s : args) {
            if (s.trim().equals("local")) {
                return true;
            }
        }
        return false;
    }

    public static void submit(String []args, String topologyName,
                              TopologyBuilder builder, TopologyConfiguration configuration) throws Exception {
        Config conf = new Config();
        if (!isLocal(args)) {
            conf.setNumWorkers(configuration.getNoWorkers());
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Thread.sleep(6000000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
    }

    public static void submit(String []args, String topologyName,
                              TopologyBuilder builder, TopologyConfiguration configuration, Config conf) throws Exception {
        if (!isLocal(args)) {
            conf.setNumWorkers(configuration.getNoWorkers());
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Thread.sleep(6000000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
    }
}
