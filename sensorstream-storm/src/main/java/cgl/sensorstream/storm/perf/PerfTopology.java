package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        JmsSpout spout = new JmsSpout();
        if (args.length == 3) {
            spout.setJmsProvider(new LocalJMSProvider(args[1], Integer.parseInt(args[2])));
        } else {
            spout.setJmsProvider(new LocalJMSProvider());
        }

        spout.setJmsTupleProducer(new JmsTupleProducer() {
            @Override
            public Values toTuple(Message message) throws JMSException {
                if (message instanceof TextMessage) {
                    long time = message.getLongProperty("time");
                    return new Values(System.currentTimeMillis() - time);
                } else {
                    return null;
                }
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
                outputFieldsDeclarer.declare(new Fields("time"));
            }
        });

        // spout.setJmsAcknowledgeMode(Session.);

        builder.setSpout("word", spout, 4);
        builder.setBolt("time1", new PerfAggrBolt(), 4).shuffleGrouping("word");

        Config conf = new Config();
        conf.setDebug(true);

//        if (args != null && args.length > 0) {
        conf.setNumWorkers(8);
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
//        } else {
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("perf", conf, builder.createTopology());
//            Thread.sleep(60000);
//            cluster.killTopology("perf");
//            cluster.shutdown();
//        }
    }

    private static class LocalJMSProvider implements JmsProvider {
        ActiveMQConnectionFactory connectionFactory;
        Destination[] destination;

        String url;
        int number;

        private LocalJMSProvider(String url, int number) {
            this.url = url;
            this.number = number;
            destination = new Destination[number];
            this.connectionFactory = new ActiveMQConnectionFactory(url);
            connectionFactory.setOptimizeAcknowledge(true);
            connectionFactory.setAlwaysSessionAsync(false);

            Connection connection;
            try {
                connection = connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                for (int i = 0; i < number; i++) {
                    this.destination[i] = session.createQueue("send" + i);
                }
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        private LocalJMSProvider() {
            this("tcp://10.39.1.55:61616", 1);
        }

        @Override
        public ConnectionFactory connectionFactory() throws Exception {
            return connectionFactory;
        }

        @Override
        public List<Destination> destination() throws Exception {
            return new ArrayList<Destination>(Arrays.asList(destination));
        }
    }
}
