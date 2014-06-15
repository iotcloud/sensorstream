package cgl.sensorstream.storm.perf;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ss.kestrel.*;
import com.ss.kestrel.bolt.KestrelBolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class KestrelPerfTopology extends AbstractPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        TopologyConfiguration configuration = parseArgs(args[0], null);
        int i = 0;
        for (Endpoint ip : configuration.getEndpoints()) {
            // todo
            KestrelSpout spout = new KestrelSpout(new SpoutConfigurator(configuration, null));
            KestrelBolt bolt = new KestrelBolt(new BoltConfigurator(configuration, null));
            builder.setSpout("kestrel_spout_" + i, spout, configuration.getParallism());
            builder.setBolt("kestrel_bolt_" + i, bolt, configuration.getParallism()).shuffleGrouping("kestrel_spout_" + i);
            i++;
        }
        submit(args, "kestrelTest", builder, configuration);
    }

    private static class TimeStampMessageBuilder implements KestrelMessageBuilder {
        @Override
        public List<Object> deSerialize(KestrelMessage envelope) {
            try {
                byte []body = envelope.getData();
                String bodyS = new String(body);
                List<Object> tuples = new ArrayList<Object>();
                long currentTime = System.currentTimeMillis();
                if (!bodyS.trim().equals("")) {
                    BufferedReader reader = new BufferedReader(new StringReader(bodyS));
                    String timeStampS = reader.readLine();
                    Long timeStamp = Long.parseLong(timeStampS);



                    System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
                    tuples.add(envelope);
                    return tuples;
                } else {
                    tuples.add(new KestrelMessage((currentTime + "\r\nhello").getBytes(), envelope.getId(), envelope.getQueue()));
                    return tuples;
                }
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

        String host;

        int port = 2229;

        private SpoutConfigurator(TopologyConfiguration configuration, String ip) {
            this.configuration = configuration;

            if (ip.contains(":")) {
                host = ip.substring(0, ip.indexOf(":"));
                port = Integer.parseInt(ip.substring(ip.indexOf(":") + 1));
            } else {
                host = ip;
            }
        }

        @Override
        public String getHost() {
            return host;
        }

        @Override
        public int getPort() {
            return port;
        }

        public int ackMode() {
            return 0;
        }

        public List<String> destinations() {
            List<String> destinations = new ArrayList<String>();
//            for (int i = 0; i < configuration.getNoQueues(); i++) {
//                destinations.add(configuration.getRecv() + "_" + i);
//            }
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
        public int expirationTime() {
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

        String host;

        int port;

        private BoltConfigurator(TopologyConfiguration configuration, String ip) {
            this.configuration = configuration;

            this.configuration = configuration;

            if (ip.contains(":")) {
                host = ip.substring(0, ip.indexOf(":"));
                port = Integer.parseInt(ip.substring(ip.indexOf(":") + 1));
            } else {
                host = ip;
            }
        }

        @Override
        public String getHost() {
            return host;
        }

        @Override
        public int getPort() {
            return port;
        }

        public int ackMode() {
            return 0;
        }

        public List<String> destinations() {
            List<String> destinations = new ArrayList<String>();
//            for (int i = 0; i < configuration.getNoQueues(); i++) {
//                destinations.add(configuration.getSend() + "_" + i);
//            }
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
        public int expirationTime() {
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
                return configuration.getSend() + "_" + queueNumber;
            }
            return null;
        }
    }
}
