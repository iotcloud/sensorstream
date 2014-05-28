package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ss.jms.*;
import com.ss.jms.bolt.JMSBolt;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JMSPerfTopology extends AbstractPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        TopologyConfiguration configuration = parseArgs(args);

        JMSSpout spout = new JMSSpout(new SpoutConfigurator(configuration), null);
        JMSBolt bolt = new JMSBolt(new BoltConfigurator(configuration), null);

        builder.setSpout("jms_spout", spout, 1);
        builder.setBolt("jms_bolt", bolt, 1).shuffleGrouping("jms_spout");

        Config conf = new Config();
//        if (args != null && args.length > 0) {
//            conf.setNumWorkers(4);
//            StormSubmitter.submitTopology("test", conf, builder.createTopology());
//        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("jmsTest", conf, builder.createTopology());
            Thread.sleep(6000000);
            cluster.killTopology("jmsTest");
            cluster.shutdown();
//        }
    }

    private static class TimeStampMessageBuilder implements MessageBuilder {
        @Override
        public List<Object> deSerialize(JMSMessage envelope) {
            try {
                Long timeStamp = envelope.getMessage().getJMSTimestamp();
                long currentTime = System.currentTimeMillis();

                System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
                List<Object> tuples = new ArrayList<Object>();
                tuples.add(envelope);
                return tuples;
            } catch (JMSException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public JMSMessage serialize(Tuple tuple, Object o) {
            if (o instanceof Session) {
                JMSMessage jmsMessage = (JMSMessage) tuple.getValue(0);
                try {
                    TextMessage message = ((Session) o).createTextMessage();
                    if (jmsMessage.getMessage() instanceof TextMessage) {
                        message.setText(((TextMessage) jmsMessage.getMessage()).getText());
                    }
                    return new JMSMessage(message, jmsMessage.getQueue());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    private static class SpoutConfigurator implements JMSConfigurator {
        TopologyConfiguration configuration;

        ActiveMQConnectionFactory connectionFactory;

        Map<String, Destination> destinations;

        private SpoutConfigurator(TopologyConfiguration configuration) {
            this.configuration = configuration;

            destinations = new HashMap<String, Destination>();
            this.connectionFactory = new ActiveMQConnectionFactory(configuration.getIp());
            connectionFactory.setOptimizeAcknowledge(true);
            connectionFactory.setAlwaysSessionAsync(false);
            Connection connection;
            try {
                connection = connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                for (int i = 0; i < configuration.getNoQueues(); i++) {
                    this.destinations.put(configuration.getRecevBaseQueueName() + "_" + i,
                            session.createQueue(configuration.getRecevBaseQueueName() + "_" + i));
                }

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

        @Override
        public JMSDestinationSelector getDestinationSelector() {
            return null;
        }
    }

    private static class BoltConfigurator implements JMSConfigurator {
        TopologyConfiguration configuration;

        ActiveMQConnectionFactory connectionFactory;

        Map<String, Destination> destinations;

        private BoltConfigurator(TopologyConfiguration configuration) {
            this.configuration = configuration;

            destinations = new HashMap<String, Destination>();
            this.connectionFactory = new ActiveMQConnectionFactory(configuration.getIp());
            connectionFactory.setOptimizeAcknowledge(true);
            connectionFactory.setAlwaysSessionAsync(false);
            Connection connection;
            try {
                connection = connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                for (int i = 0; i < configuration.getNoQueues(); i++) {
                    this.destinations.put(configuration.getSendBaseQueueName() + "_" + i,
                            session.createQueue(configuration.getSendBaseQueueName() + "_" + i));
                }

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
            outputFieldsDeclarer.declare(new Fields("body"));
        }

        public int queueSize() {
            return 1024;
        }

        @Override
        public JMSDestinationSelector getDestinationSelector() {
            return new PerfDestinationSelector(configuration);
        }
    }

    private static class PerfDestinationSelector implements JMSDestinationSelector {
        private TopologyConfiguration configuration;

        private PerfDestinationSelector(TopologyConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public String select(Tuple tuple) {
            JMSMessage mqttMessage = (JMSMessage) tuple.getValue(0);
            String queue = mqttMessage.getQueue();
            if (queue != null) {
                String queueNumber = queue.substring(queue.indexOf("_") + 1);
                return configuration.getSendBaseQueueName() + "_" + queueNumber;
            }
            return null;
        }
    }
}
