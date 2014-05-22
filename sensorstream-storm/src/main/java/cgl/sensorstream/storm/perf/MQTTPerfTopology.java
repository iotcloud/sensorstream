package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ss.mqtt.*;
import com.ss.mqtt.bolt.MQTTBolt;
import org.fusesource.mqtt.client.QoS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class MQTTPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        MQTTSpout spout = new MQTTSpout(new SpoutConfigurator(), null);
        MQTTBolt bolt = new MQTTBolt(new BoltConfigurator());

        builder.setSpout("word", spout, 1);
        builder.setBolt("time1", bolt, 1).shuffleGrouping("word");

        Config conf = new Config();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(4);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("mqttTest", conf, builder.createTopology());
            Thread.sleep(6000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    private static class TimeStampMessageBuilder implements MessageBuilder {
        @Override
        public List<Object> deSerialize(MQTTMessage envelope) {
            try {
                byte []body = envelope.getBody().toByteArray();
                String bodyS = new String(body);
                BufferedReader reader = new BufferedReader(new StringReader(bodyS));
                String timeStampS = reader.readLine();
                Long timeStamp = Long.parseLong(timeStampS);

                long currentTime = System.currentTimeMillis();

                System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
                List<Object> tuples = new ArrayList<Object>();
                tuples.add(new Long((currentTime - timeStamp)));
                tuples.add(envelope);

                return tuples;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public MQTTMessage serialize(Tuple tuple) {
            Object message = tuple.getValue(1);
            if (message instanceof  MQTTMessage){
                return (MQTTMessage) message;
            }
            return null;
        }
    }

    private static class SpoutConfigurator implements MQTTConfigurator {
        private String url = "tcp://localhost:61616";

        private String queueName = "send";

        public MessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        @Override
        public QoS qosLevel() {
            return QoS.AT_MOST_ONCE;
        }

        @Override
        public String getURL() {
            return url;
        }

        @Override
        public List<String> getQueueName() {
            return Arrays.asList(queueName);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("time1"));
        }

        public int queueSize() {
            return 1024;
        }
    }

    private static class BoltConfigurator implements MQTTConfigurator {
        private String url = "tcp://localhost:61616";

        private String queueName = "send";

        public MessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        @Override
        public QoS qosLevel() {
            return QoS.AT_MOST_ONCE;
        }

        @Override
        public String getURL() {
            return url;
        }

        @Override
        public List<String> getQueueName() {
            return Arrays.asList(queueName);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("body"));
        }

        public int queueSize() {
            return 1024;
        }
    }
}
