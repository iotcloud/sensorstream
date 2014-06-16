package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.spout.MultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableMap;
import storm.kafka.*;
import storm.kafka.bolt.BoltConfig;
import storm.kafka.bolt.KafkaBolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;


public class KafkaPerfTopology extends AbstractPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();


        Config config = new Config();
//        Map<String, String> options = ImmutableMap.of("zip", "Zookeeper hosts", "zport", "zookeeper port");
        TopologyConfiguration configuration = parseArgs(args[0], null);

        int i = 0;
        for (Endpoint ip : configuration.getEndpoints()) {
            for (String iot : ip.getIotServers()) {

                String zooIps = ip.getProperties().get("zkIp");
                int zport = Integer.parseInt(ip.getProperties().get("zPort"));
                List<String> zooServers = getZooServers(zooIps);

                BrokerHosts brokerHosts = new ZkHosts(zkConnectionString(zooServers, zport));
                SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, iot + "." +  configuration.getRecv(), "", "kafka_spout");

                spoutConfig.zkServers = zooServers;
                spoutConfig.zkPort = zport;
                spoutConfig.scheme = new TimeStampMessageBuilder();

                KafkaSpout spout = new KafkaSpout(spoutConfig);
                builder.setSpout("kafka_spout_" + i, spout, configuration.getParallism());


                Map<String, Object> props = new HashMap<String, Object>();
                // todo fix
                props.put("metadata.broker.list", ip.getUrl());
                BoltConfig boltConfig = new BoltConfig(iot + "." + configuration.getSend(), props);
                KafkaBolt bolt = new KafkaBolt(boltConfig);
                // config.put(KafkaBolt.TOPIC, iot + "." + configuration.getSend());

                // config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
                builder.setBolt("kafka_bolt_" + i, bolt, configuration.getParallism()).shuffleGrouping("kafka_spout_" + i);
                i++;
            }
        }

        submit(args, "kafkaTest", builder, configuration, config);
    }

    private static List<String> getZooServers(String zooIps) {
        String zooIpProps[] = zooIps.split(",");
        List<String> list = new ArrayList<String>();
        Collections.addAll(list, zooIpProps);
        return list;
    }

    private static class TimeStampMessageBuilder implements MultiScheme {
        protected double averageLatency = 0;

        long count = 0;

        public void calculateAverage(long val) {
            count++;
            double delta = val - averageLatency;
            averageLatency = averageLatency + delta / count;
        }

        @Override
        public Iterable<List<Object>> deserialize(byte[] body) {
            try {
                String bodyS = new String(body);
                long currentTime = System.currentTimeMillis();
                BufferedReader reader = new BufferedReader(new StringReader(bodyS));
                String timeStampS = reader.readLine();
                Long timeStamp = Long.parseLong(timeStampS);
                calculateAverage(currentTime - timeStamp);
                System.out.println("latency: " + averageLatency + " initial time: " + timeStamp + " current: " + currentTime);
                List<Object> tuples = new ArrayList<Object>();
                tuples.add(body);
                tuples.add("key1".getBytes());
                return Arrays.asList(tuples);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("message", "key");
        }
    }

    private static String brokerList(List<String> brokers) {
        String ret = "";
        for (String s : brokers) {
            ret = ret + s + ",";
        }
        return ret;
    }

    private static String zkConnectionString(List<String> brokers, int port) {
        String str = "";
        for (String b : brokers) {
            str += b + ":" + port + ",";
        }
        if (str.length() > 0 && str.charAt(str.length() - 1) == 'x') {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }
}
