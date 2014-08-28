package cgl.sensorstream.examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class WCountTopology {
    public static class RandomSentenceSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        Random _rand;


        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = new Random();
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
                    "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
            String sentence = sentences[_rand.nextInt(sentences.length)];
            _collector.emit(Arrays.<Object>asList(sentence.getBytes(), "aaa", "123"));
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "sensorID", "time"));
        }

    }

    public static class SplitSentence extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "sensorID", "time"));
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String sentence = new String((byte [])tuple.getValue(0));
            String words[] = sentence.split(" ");
            Object sensorId = tuple.getValue(1);
            Object time = tuple.getValue(2);
            if (words != null) {
                for (String w : words) {
                    basicOutputCollector.emit(Arrays.<Object>asList(w, sensorId, time));
                }
            }
        }
    }

    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            Object sensorId = tuple.getValue(1);
            Object time = tuple.getString(2);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            String out = word + ":" + count;
            collector.emit(Arrays.<Object>asList(out.getBytes(), sensorId, time));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("body", "sensorID", "time"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

//        StreamTopologyBuilder streamTopologyBuilder = new StreamTopologyBuilder("kafka_topology.yaml");
//        StreamTopologyBuilder streamTopologyBuilder = new StreamTopologyBuilder();
//        StreamComponents components = streamTopologyBuilder.buildComponents();
        //builder.setSpout("spout", components.getSpouts().get("sentence_receive"), 1);
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));
//        builder.setBolt("sender", components.getBolts().get("count_send")).shuffleGrouping("count");



        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", conf, builder.createTopology());
            Thread.sleep(20000);
            cluster.shutdown();
        }
    }
}
