package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ss.jms.*;
import com.ss.jms.bolt.JMSBolt;
import org.apache.activemq.ActiveMQConnectionFactory;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;
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
        KafkaSpout spout = new KafkaSpout(spoutConfig);
        builder.setSpout("kafka_spout" + i, spout, 1);

        KafkaBolt bolt = new KafkaBolt();
        config.put(KafkaBolt.TOPIC, configuration.getSendBaseQueueName());
        config.put(KafkaBolt.BOLT_KEY, "key");
        config.put(KafkaBolt.BOLT_MESSAGE, "message");
        builder.setBolt("kafka_bolt", bolt, 1).shuffleGrouping("kafka_bolt_" + i);

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
}
