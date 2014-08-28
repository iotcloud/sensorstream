package cgl.sensorstream.core.kafka;

import backtype.storm.tuple.Tuple;
import cgl.iotcloud.core.api.thrift.TSensorMessage;
import cgl.iotcloud.core.transport.TransportConstants;
import cgl.iotcloud.core.utils.SerializationUtils;
import com.rabbitmq.client.AMQP;
import com.ss.commons.MessageBuilder;
import com.ss.commons.MessageContext;
import com.ss.kafka.KafkaMessage;
import com.ss.rabbitmq.RabbitMQMessage;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultKafkaMessageBuilder implements MessageBuilder {
    @Override
    public List<Object> deSerialize(Object o) {
        List<Object> tuples = new ArrayList<Object>();
        if (o instanceof MessageContext) {
            if (((MessageContext) o).getMessage() instanceof byte []) {
                byte [] byteMessage = (byte[]) ((MessageContext) o).getMessage();
                TSensorMessage sensorMessage = new TSensorMessage();
                try {
                    SerializationUtils.createThriftFromBytes(byteMessage, sensorMessage);
                } catch (TException e) {
                    e.printStackTrace();
                }

                String sensorId = sensorMessage.getSensorId();
                byte []message = sensorMessage.getBody();

                tuples.add(message);
                if (sensorId != null) {
                    tuples.add(sensorId);
                }
                if (sensorMessage.getProperties() != null) {
                    String time = sensorMessage.getProperties().get("time");
                    if (time !=  null) {
                        tuples.add(time);
                    } else {
                        tuples.add(Long.toString(System.currentTimeMillis()));
                    }
                } else {
                    tuples.add(Long.toString(System.currentTimeMillis()));
                }

            }
        }
        return tuples;
    }

    @Override
    public Object serialize(Tuple tuple, Object o) {
        TSensorMessage message = new TSensorMessage();

        byte []body = (byte[]) tuple.getValueByField("body");
        Object sensorId = tuple.getValueByField(TransportConstants.SENSOR_ID);
        Object time = tuple.getValueByField("time");

        message.setBody(body);
        message.setSensorId(sensorId.toString());
        message.putToProperties("time", time.toString());

        try {
            byte []msg = SerializationUtils.serializeThriftObject(message);
            return new KafkaMessage(sensorId.toString(), msg);
        } catch (TException e) {
            e.printStackTrace();
        }
        return null;
    }
}
