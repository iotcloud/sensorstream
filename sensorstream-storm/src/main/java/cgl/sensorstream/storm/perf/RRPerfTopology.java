package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.ss.mqtt.DestinationSelector;
import com.ss.mqtt.MQTTMessage;
import com.ss.rabbitmq.*;
import com.ss.rabbitmq.bolt.RabbitMQBolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RRPerfTopology extends AbstractPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        TopologyConfiguration configuration = parseArgs(args);

        ErrorReporter r = new ErrorReporter() {
            @Override
            public void reportError(Throwable t) {
                t.printStackTrace();
            }
        };

        RabbitMQSpout spout = new RabbitMQSpout(new SpoutConfigurator(configuration), r);
        RabbitMQBolt bolt = new RabbitMQBolt(new BoltConfigurator(configuration), r);

        builder.setSpout("rabbit_spout", spout, 1);
        builder.setBolt("rabbit_bolt", bolt, 2).shuffleGrouping("rabbit_spout");

        Config conf = new Config();
//        if (args != null && args.length > 0) {
            //conf.setNumWorkers(6);
            //StormSubmitter.submitTopology("test", conf, builder.createTopology());
//        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Thread.sleep(60000);
            cluster.killTopology("test");
            cluster.shutdown();
//        }
    }

    private static class TimeStampMessageBuilder implements MessageBuilder {
        @Override
        public List<Object> deSerialize(RabbitMQMessage message) {
            Map<String, Object> headers = message.getProperties().getHeaders();
            Long timeStamp = (Long) headers.get("time");
            long currentTime = System.currentTimeMillis();

            System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
            List<Object> tuples = new ArrayList<Object>();
            tuples.add(message);
            return tuples;
        }

        @Override
        public RabbitMQMessage serialize(Tuple tuple) {
            Object message = tuple.getValue(0);
            if (message instanceof  RabbitMQMessage){
                return (RabbitMQMessage) message;
            }
            return null;
        }
    }

    private static class SpoutConfigurator implements RabbitMQConfigurator {
        private String url = "amqp://localhost:5672";

        private TopologyConfiguration configuration;

        private SpoutConfigurator(TopologyConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public String getURL() {
            return configuration.getIp();
        }

        @Override
        public boolean isAutoAcking() {
            return true;
        }

        @Override
        public int getPrefetchCount() {
            return 1024;
        }

        @Override
        public boolean isReQueueOnFail() {
            return false;
        }

        @Override
        public String getConsumerTag() {
            return "sender";
        }

        @Override
        public List<RabbitMQDestination> getQueueName() {
            List<RabbitMQDestination> list = new ArrayList<RabbitMQDestination>();
            for (int i = 0; i < configuration.getNoQueues(); i++) {
                list.add(new RabbitMQDestination(configuration.getRecevBaseQueueName() + "_" + i));
            }
            return list;
        }

        @Override
        public MessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("time1"));
        }

        @Override
        public int queueSize() {
            return 1024;
        }

        @Override
        public RabbitMQDestinationSelector getDestinationSelector() {
            return null;
        }
    }

    private static class BoltConfigurator implements RabbitMQConfigurator {
        private String url = "amqp://localhost:5672";

        private TopologyConfiguration configuration;

        private BoltConfigurator(TopologyConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public String getURL() {
            return configuration.getIp();
        }

        @Override
        public boolean isAutoAcking() {
            return false;
        }

        @Override
        public int getPrefetchCount() {
            return 1024;
        }

        @Override
        public boolean isReQueueOnFail() {
            return false;
        }

        @Override
        public String getConsumerTag() {
            return "sender";
        }

        @Override
        public List<RabbitMQDestination> getQueueName() {
            List<RabbitMQDestination> list = new ArrayList<RabbitMQDestination>();
            for (int i = 0; i < configuration.getNoQueues(); i++) {
                list.add(new RabbitMQDestination(configuration.getSendBaseQueueName() + "_" + i, "perf", configuration.getSendBaseQueueName() + "_" + i));
            }
            return list;
        }

        @Override
        public MessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("time1"));
        }

        @Override
        public int queueSize() {
            return 1024;
        }

        @Override
        public RabbitMQDestinationSelector getDestinationSelector() {
            return new RabbitMQDestinationSelector() {
                @Override
                public String select(Tuple message) {
                    RabbitMQMessage mqttMessage = (RabbitMQMessage) message.getValue(0);
                    String queue = mqttMessage.getQueue();
                    if (queue != null) {
                        String queueNumber = queue.substring(queue.indexOf("_") + 1);
                        return configuration.getSendBaseQueueName() + "_" + queueNumber;
                    }
                    return null;
                }
            };
        }
    }
}
