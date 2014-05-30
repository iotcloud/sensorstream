package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ss.kestrel.*;
import com.ss.kestrel.bolt.KestrelBolt;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KestrelPerfTopology extends AbstractPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        TopologyConfiguration configuration = parseArgs(args);

        KestrelSpout spout = new KestrelSpout(new SpoutConfigurator(configuration));
        KestrelBolt bolt = new KestrelBolt(new BoltConfigurator(configuration));

        builder.setSpout("kestrel_spout", spout, 1);
        builder.setBolt("kestrel_bolt", bolt, 1).shuffleGrouping("kestrel_spout");

        Config conf = new Config();
//        if (args != null && args.length > 0) {
//            conf.setNumWorkers(4);
//            StormSubmitter.submitTopology("test", conf, builder.createTopology());
//        } else {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kestrelTest", conf, builder.createTopology());
        Thread.sleep(6000000);
        cluster.killTopology("kestrelTest");
        cluster.shutdown();
//        }
    }

    private static class TimeStampMessageBuilder implements KestrelMessageBuilder {
        @Override
        public List<Object> deSerialize(KestrelMessage envelope) {
            try {
                byte []body = envelope.getData();
                String bodyS = new String(body);
                BufferedReader reader = new BufferedReader(new StringReader(bodyS));
                String timeStampS = reader.readLine();
                Long timeStamp = Long.parseLong(timeStampS);

                long currentTime = System.currentTimeMillis();

                System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
                List<Object> tuples = new ArrayList<Object>();
                tuples.add(envelope);
                return tuples;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public KestrelMessage serialize(Tuple tuple) {
            KestrelMessage message = (KestrelMessage) tuple.getValue(0);
            return message;
        }
    }

    private static class SpoutConfigurator implements KestrelConfigurator {
        TopologyConfiguration configuration;

        private SpoutConfigurator(TopologyConfiguration configuration) {
            this.configuration = configuration;
        }

        public int ackMode() {
            return 0;
        }

        public Map<String, KestrelDestination> destinations() {
            Map<String, KestrelDestination> destinations = new HashMap<String, KestrelDestination>();
            return destinations;
        }

        public KestrelMessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("time1"));
        }

        public int queueSize() {
            return 1024;
        }

        @Override
        public long expirationTime() {
            return 30000;
        }

        @Override
        public long blackListTime() {
            return 30000;
        }

        @Override
        public int timeOut() {
            return 30;
        }

        @Override
        public KestrelDestinationSelector getDestinationSelector() {
            return null;
        }
    }

    private static class BoltConfigurator implements KestrelConfigurator {
        TopologyConfiguration configuration;

        private BoltConfigurator(TopologyConfiguration configuration) {
            this.configuration = configuration;
        }

        public int ackMode() {
            return 0;
        }

        public Map<String, KestrelDestination> destinations() {
            Map<String, KestrelDestination> destinations = new HashMap<String, KestrelDestination>();
            return destinations;
        }

        public KestrelMessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("time1"));
        }

        public int queueSize() {
            return 1024;
        }

        @Override
        public long expirationTime() {
            return 30000;
        }

        @Override
        public long blackListTime() {
            return 30000;
        }

        @Override
        public int timeOut() {
            return 30;
        }

        @Override
        public KestrelDestinationSelector getDestinationSelector() {
            return new PerfDestinationSelector(configuration);
        }
    }

    private static class PerfDestinationSelector implements KestrelDestinationSelector {
        private TopologyConfiguration configuration;

        private PerfDestinationSelector(TopologyConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public String select(Tuple tuple) {
            KestrelMessage message = (KestrelMessage) tuple.getValue(0);
            String queue = message.getQueue();
            if (queue != null) {
                String queueNumber = queue.substring(queue.indexOf("_") + 1);
                return configuration.getSendBaseQueueName() + "_" + queueNumber;
            }
            return null;
        }
    }
}
