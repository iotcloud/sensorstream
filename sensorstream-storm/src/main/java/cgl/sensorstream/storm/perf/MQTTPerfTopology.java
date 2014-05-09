package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.ss.jms.JMSConfigurator;
import com.ss.jms.JMSSpout;
import com.ss.jms.MessageBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MQTTPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        JMSSpout spout = new JMSSpout(new Configurator("tcp://localhost:61616", 100), null);
        builder.setSpout("word", spout, 1);
        builder.setBolt("time1", new PerfAggrBolt(), 1).shuffleGrouping("word");

        Config conf = new Config();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(4);
            StormSubmitter.submitTopology("test", conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Thread.sleep(6000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    private static class TimeStampMessageBuilder implements MessageBuilder {
        @Override
        public List<Object> deSerialize(Message envelope) {
            Long timeStamp = null;
            try {
                timeStamp = envelope.getJMSTimestamp();
                long currentTime = System.currentTimeMillis();

                System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
                List<Object> tuples = new ArrayList<Object>();
                tuples.add(new Long((currentTime - timeStamp)));
                return tuples;
            } catch (JMSException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private static class Configurator implements JMSConfigurator {
        private String url = "tcp://localhost:61616";

        private String queueName = "send";

        ActiveMQConnectionFactory connectionFactory;
        Map<String, Destination> destinations;

        int number;

        private Configurator(String url, int number) {
            this.url = url;
            this.number = number;
            destinations = new HashMap<String, Destination>();
            this.connectionFactory = new ActiveMQConnectionFactory(url);
            connectionFactory.setOptimizeAcknowledge(true);
            connectionFactory.setAlwaysSessionAsync(false);

            Connection connection;
            try {
                connection = connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//                for (int i = 0; i < number; i++) {
                this.destinations.put(queueName, session.createQueue(queueName));
//                }

                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        public int ackMode() {
            return Session.AUTO_ACKNOWLEDGE;
        }

        public ConnectionFactory connectionFactory() throws Exception {
            return connectionFactory;
        }

        public Map<String, Destination> destinations() throws Exception {
            return destinations;
        }

        public MessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("time1"));
        }

        public int queueSize() {
            return 1024;
        }
    }
}
