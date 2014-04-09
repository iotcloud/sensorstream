package cgl.sensorstream.storm.perf;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PerfAggrBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(PerfAggrBolt.class);
    OutputCollector _collector;

    double averageLatency = 0;

    long count = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        Long val = (Long) tuple.getValue(0);
        count++;
        double delta = val - averageLatency;
        averageLatency = averageLatency + delta / count;
        _collector.emit(new Values(averageLatency));

        LOG.info("The latency: " + averageLatency + " count: " + count + " val: " + val);
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time"));
    }
}