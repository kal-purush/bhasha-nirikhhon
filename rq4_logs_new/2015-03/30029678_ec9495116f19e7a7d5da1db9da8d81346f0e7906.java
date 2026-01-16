package cgl.iotrobots.slam.streaming;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cgl.iotrobots.slam.streaming.msgs.Ready;
import cgl.iotrobots.utils.rabbitmq.*;
import cgl.sensorstream.core.StreamComponents;
import cgl.sensorstream.core.StreamTopologyBuilder;
import com.esotericsoftware.kryo.Kryo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DispatcherBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(DispatcherBolt.class);

    // output collector
    private OutputCollector outputCollector;

    // topology context
    private TopologyContext topologyContext;

    // use this receiver to synchronize the
    private RabbitMQReceiver readyReceiver;

    // broker url
    private String brokerURL = "amqp://localhost:5672";

    private Kryo kryo;

    private StreamTopologyBuilder streamTopologyBuilder;
    private StreamComponents components;
    private Map conf;

    private int noOfParallelTasks = 0;

    private Tuple currentTuple;

    private Lock lock = new ReentrantLock();

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constants.Fields.SCAN_STREAM, new Fields(
                Constants.Fields.BODY,
                Constants.Fields.SENSOR_ID_FIELD,
                Constants.Fields.TIME_FIELD));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.conf = stormConf;
        this.topologyContext = context;
        this.outputCollector = collector;
        streamTopologyBuilder = new StreamTopologyBuilder();
        components = streamTopologyBuilder.buildComponents();

        this.brokerURL = (String) components.getConf().get(Constants.RABBITMQ_URL);
        if (conf.get(Constants.ARGS_PARALLEL) != null) {
            this.noOfParallelTasks = (int) conf.get(Constants.ARGS_PARALLEL);
        } else {
            this.noOfParallelTasks = topologyContext.getComponentTasks(Constants.Topology.SCAN_MATCH_BOLT).size();
        }

        kryo = new Kryo();
        Utils.registerClasses(kryo);

        try {
            this.readyReceiver = new RabbitMQReceiver(brokerURL, Constants.Messages.DIRECT_EXCHANGE);
            this.readyReceiver.listen(new ReadyMessageListener());
        } catch (Exception e) {
            String msg = "Failed to create the receiver";
            LOG.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    private List<Ready> readyList = new ArrayList<Ready>();

    @Override
    public void execute(Tuple input) {
        lock.lock();
        try {
            if (this.currentTuple == null) {
                outputCollector.emit(Constants.Fields.SCAN_STREAM, createTuple(input));
            }
            this.currentTuple = input;
        } finally {
            lock.unlock();
        }
    }

    private class ReadyMessageListener implements MessageHandler {
        @Override
        public Map<String, String> getProperties() {
            Map<String, String> props = new HashMap<String, String>();
            props.put(MessagingConstants.RABBIT_QUEUE, Constants.Messages.READY_ROUTING_KEY);
            props.put(MessagingConstants.RABBIT_ROUTING_KEY, Constants.Messages.READY_ROUTING_KEY);
            return props;
        }

        @Override
        public void onMessage(Message message) {
            byte []messageBody = message.getBody();
            Ready ready = (Ready) Utils.deSerialize(kryo, messageBody, Ready.class);

            lock.lock();
            readyList.add(ready);
            try {
                if (readyList.size() == noOfParallelTasks) {
                    List<Object> emit = createTuple(currentTuple);

                    outputCollector.emit(Constants.Fields.SCAN_STREAM, emit);
                    readyList.clear();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private List<Object> createTuple(Tuple currentTuple) {
        Object body = currentTuple.getValueByField(Constants.Fields.BODY);
        Object time = currentTuple.getValueByField(Constants.Fields.TIME_FIELD);
        Object sensorId = currentTuple.getValueByField(Constants.Fields.SENSOR_ID_FIELD);

        List<Object> emit = new ArrayList<Object>();
        emit.add(body);
        emit.add(sensorId);
        emit.add(time);
        return emit;
    }
}