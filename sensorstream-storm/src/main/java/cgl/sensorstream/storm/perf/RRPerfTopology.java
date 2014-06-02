package cgl.sensorstream.storm.perf;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.rabbitmq.client.AMQP;
import com.ss.rabbitmq.*;
import com.ss.rabbitmq.bolt.RabbitMQBolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
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

        int i = 0;
        for (String ip : configuration.getIp()) {
            RabbitMQSpout spout = new RabbitMQSpout(new SpoutConfigurator(configuration, ip), r);
            RabbitMQBolt bolt = new RabbitMQBolt(new BoltConfigurator(configuration, ip), r);
            builder.setSpout("rabbit_spout_" + i, spout, 1);
            builder.setBolt("rabbit_bolt_" + i, bolt, 2).shuffleGrouping("rabbit_spout_" + i);
            i++;
        }
        submit(args, "rabbitTest", builder, configuration);
    }

    private static class TimeStampMessageBuilder implements MessageBuilder {
        @Override
        public List<Object> deSerialize(RabbitMQMessage message) {
            byte []body = message.getBody();
            String bodyS = new String(body);
            BufferedReader reader = new BufferedReader(new StringReader(bodyS));
            String timeStampS = null;
            try {
                timeStampS = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Long timeStamp = Long.parseLong(timeStampS);
            long currentTime = System.currentTimeMillis();
            System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
            List<Object> tuples = new ArrayList<Object>();

            SendMessage sendMessage = new SendMessage(message.getQueue(), message.getBody());
            tuples.add(sendMessage);
            return tuples;
        }

        @Override
        public RabbitMQMessage serialize(Tuple tuple) {
            Object message = tuple.getValue(0);
            if (message instanceof  SendMessage){
                RabbitMQMessage rrMessage = new RabbitMQMessage(((SendMessage) message).getQueue(), null, null, null, ((SendMessage) message).getContent());
                return rrMessage;
            }
            return null;
        }
    }

    private static class SendMessage implements Serializable {
        private String queue;

        private byte[] content;

        private SendMessage(String queue, byte[] content) {
            this.queue = queue;
            this.content = content;
        }

        public String getQueue() {
            return queue;
        }

        public byte[] getContent() {
            return content;
        }
    }

    private static class SpoutConfigurator implements RabbitMQConfigurator {
        private String url = "amqp://localhost:5672";

        private TopologyConfiguration configuration;

        private String ip;

        private SpoutConfigurator(TopologyConfiguration configuration, String ip) {
            this.configuration = configuration;
            this.ip = ip;
        }

        @Override
        public String getURL() {
            return ip;
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
                list.add(new RabbitMQDestination(configuration.getRecevBaseQueueName() + "_" + i,
                        "perfSensor", configuration.getRecevBaseQueueName() + "_" + i));
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

        private String ip;

        private BoltConfigurator(TopologyConfiguration configuration, String ip) {
            this.configuration = configuration;
            this.ip = ip;
        }

        @Override
        public String getURL() {
            return ip;
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
                list.add(new RabbitMQDestination(configuration.getSendBaseQueueName() + "_" + i, "perfSensor", configuration.getSendBaseQueueName() + "_" + i));
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
                    SendMessage mqttMessage = (SendMessage) message.getValue(0);
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
