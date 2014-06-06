package cgl.sensorstream.storm.perf;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ss.jms.*;
import com.ss.jms.bolt.JMSBolt;
import org.apache.activemq.ActiveMQConnectionFactory;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import javax.jms.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaPerfTopology extends AbstractPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        TopologyConfiguration configuration = parseArgs(args);


        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        globalPartitionInformation.addPartition(0, Broker.fromString("aaaa"));
        globalPartitionInformation.addPartition(1, Broker.fromString("aaaa"));

        BrokerHosts brokerHosts = new StaticHosts(globalPartitionInformation);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, configuration.getRecevBaseQueueName(), "", "kafka_spout");

        int i = 0;
        for (String ip : configuration.getIp()) {
            KafkaSpout spout = new KafkaSpout(spoutConfig);
            builder.setSpout("kafka_spout_" + i, spout, 1);

            JMSBolt bolt = new JMSBolt(new BoltConfigurator(configuration, ip), null);
            builder.setBolt("kafka_bolt_" + i, bolt, 1).shuffleGrouping("jms_spout_" + i);
            i++;
        }

        submit(args, "jmsTest", builder, configuration);
    }

    private static class SendMessage implements Serializable {
        private String queue;

        private String content;

        public SendMessage(String queue, String content) {
            this.queue = queue;
            this.content = content;
        }

        public String getQueue() {
            return queue;
        }

        public String getContent() {
            return content;
        }
    }

    private static class TimeStampMessageBuilder implements MessageBuilder {
        @Override
        public List<Object> deSerialize(JMSMessage envelope) {
            try {
                Long timeStamp = envelope.getMessage().getJMSTimestamp();
                long currentTime = System.currentTimeMillis();

                calculateAverage(currentTime - timeStamp);
                System.out.println("latency: " + (currentTime - timeStamp) + " average: " + averageLatency);
                List<Object> tuples = new ArrayList<Object>();

                if (envelope.getMessage() instanceof TextMessage) {
                    SendMessage message = new SendMessage(envelope.getQueue(), ((TextMessage) envelope.getMessage()).getText());
                    tuples.add(message);
                }

                return tuples;
            } catch (JMSException e) {
                e.printStackTrace();
            }
            return null;
        }

        protected double averageLatency = 0;

        long count = 0;

        public void calculateAverage(long val) {
            count++;
            if (val < 0) {
                averageLatency = 0;
                count = 0;
            } else {
                double delta = val - averageLatency;
                averageLatency = averageLatency + delta / count;
            }
        }

        @Override
        public JMSMessage serialize(Tuple tuple, Object o) {
            if (o instanceof Session) {
                SendMessage jmsMessage = (SendMessage) tuple.getValue(0);
                try {
                    TextMessage message = ((Session) o).createTextMessage();
                    message.setText(jmsMessage.getContent());
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

        private SpoutConfigurator(TopologyConfiguration configuration, String ip) {
            this.configuration = configuration;

            destinations = new HashMap<String, Destination>();
            this.connectionFactory = new ActiveMQConnectionFactory(ip);
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
            outputFieldsDeclarer.declare(new Fields("jms_spout_out"));
        }

        public int queueSize() {
            return 1024;
        }

        @Override
        public JMSDestinationSelector getDestinationSelector() {
            return null;
        }
    }

    private static class PerfDestinationSelector implements JMSDestinationSelector {
        private TopologyConfiguration configuration;

        private PerfDestinationSelector(TopologyConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public String select(Tuple tuple) {
            SendMessage mqttMessage = (SendMessage) tuple.getValue(0);
            String queue = mqttMessage.getQueue();
            if (queue != null) {
                String queueNumber = queue.substring(queue.indexOf("_") + 1);
                return configuration.getSendBaseQueueName() + "_" + queueNumber;
            }
            return null;
        }
    }
}
