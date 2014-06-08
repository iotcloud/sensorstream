package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.spout.MultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.trident.GlobalPartitionInformation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import static backtype.storm.utils.Utils.tuple;
import static java.util.Arrays.asList;


public class KafkaPerfTopology extends AbstractPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        Config config = new Config();

        TopologyConfiguration configuration = parseArgs(args);

        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        int i = 0;
        for (String ip : configuration.getIp()) {
            globalPartitionInformation.addPartition(0, Broker.fromString(ip));
            globalPartitionInformation.addPartition(1, Broker.fromString(ip));
            i++;
        }

        BrokerHosts brokerHosts = new StaticHosts(globalPartitionInformation);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, configuration.getRecevBaseQueueName(), "", "kafka_spout");
        spoutConfig.scheme = new TimeStampMessageBuilder();

        KafkaSpout spout = new KafkaSpout(spoutConfig);
        builder.setSpout("kafka_spout" + i, spout, 1);

        KafkaBolt bolt = new KafkaBolt();
        config.put(KafkaBolt.TOPIC, configuration.getSendBaseQueueName());
        builder.setBolt("kafka_bolt", bolt, 1).shuffleGrouping("kafka_bolt_" + i);

        submit(args, "jmsTest", builder, configuration, config);
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
                return asList(tuple(body, "key1"));
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
}
