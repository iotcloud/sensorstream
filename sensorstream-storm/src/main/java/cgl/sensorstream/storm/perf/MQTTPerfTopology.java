package cgl.sensorstream.storm.perf;

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

public class MQTTPerfTopology extends AbstractPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        TopologyConfiguration configuration = parseArgs(args[0], null);
        int i = 0;
        for (Endpoint ip : configuration.getEndpoints()) {
            for (String iot : ip.getIotServers()) {
                MQTTSpout spout = new MQTTSpout(new SpoutConfigurator(iot + "." + configuration.getRecv(), ip.getUrl()), null);
                MQTTBolt bolt = new MQTTBolt(new BoltConfigurator(iot + "." + configuration.getSend(), ip.getUrl()));
                builder.setSpout("mqtt_spout_" + i, spout, 1);
                builder.setBolt("mqtt_bolt_" + i, bolt, 1).shuffleGrouping("mqtt_spout_" + i);
                i++;
            }
        }
        submit(args, "mqttTest", builder, configuration);
    }

    private static class TimeStampMessageBuilder implements MessageBuilder {
        @Override
        public List<Object> deSerialize(MQTTMessage envelope) {
            try {
                byte []body = envelope.getBody();
                String bodyS = new String(body);
                BufferedReader reader = new BufferedReader(new StringReader(bodyS));
                String timeStampS = reader.readLine();
                Long timeStamp = Long.parseLong(timeStampS);

                long currentTime = System.currentTimeMillis();

                System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
                List<Object> tuples = new ArrayList<Object>();
                BoltMessage newMessage = new BoltMessage(envelope.getQueue(), body);
                tuples.add(newMessage);
                return tuples;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public MQTTMessage serialize(Tuple tuple) {
            Object message = tuple.getValue(0);
            if (message instanceof  BoltMessage) {
                return new MQTTMessage(null, (byte [])((BoltMessage) message).getContent(),
                        ((BoltMessage) message).getQueue(), null);
            }
            return null;
        }
    }

    private static class SpoutConfigurator implements MQTTConfigurator {
        private String ip;

        private String recv;

        private SpoutConfigurator(String recv, String ip) {
            this.ip = ip;
            this.recv = recv;
        }

        public MessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        @Override
        public QoS qosLevel() {
            return QoS.AT_MOST_ONCE;
        }

        @Override
        public String getURL() {
            return ip;
        }

        @Override
        public List<String> getQueueName() {
            List<String> list = new ArrayList<String>();
            list.add(recv);
            return list;
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("time1"));
        }

        public int queueSize() {
            return 1024;
        }

        @Override
        public DestinationSelector getDestinationSelector() {
            return null;
        }
    }

    private static class BoltConfigurator implements MQTTConfigurator {
        private String ip;

        String send;

        private BoltConfigurator(String send, String ip) {
            this.ip = ip;
            this.send = send;
        }

        public MessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        @Override
        public QoS qosLevel() {
            return QoS.AT_MOST_ONCE;
        }

        @Override
        public String getURL() {
            return ip;
        }

        @Override
        public List<String> getQueueName() {
            List<String> list = new ArrayList<String>();
            list.add(send);
            return list;
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("body"));
        }

        public int queueSize() {
            return 1024;
        }

        @Override
        public DestinationSelector getDestinationSelector() {
            return new DestinationSelector() {
                @Override
                public String select(Tuple message) {
                    return send;
                }
            };
        }
    }
}
