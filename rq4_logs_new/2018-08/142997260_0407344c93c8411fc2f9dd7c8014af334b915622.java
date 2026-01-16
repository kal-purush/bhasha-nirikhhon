package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import utils.CalculateCache;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 从classifyBolt-->geThan中取出大于等于50的数据，计算总数，每1分钟计算一次
 * @author wangyj
 * @description
 * @create 2018-08-21 9:52
 **/
public class CalcluateGeThanBolt extends BaseRichBolt {

    private OutputCollector collector;

    CalculateCache cache;

    //设定计算时间
    int timeSection;
    //计算线程描述
    String desc;

    public CalcluateGeThanBolt(int timeSection, String desc){
        this.timeSection = timeSection;
        this.desc = desc;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        cache = new CalculateCache(timeSection, new CalcluateGeThanBoltExpiredCallback());
    }

    public void execute(Tuple tuple) {
        cache.put(tuple.getIntegerByField("gt"));
        collector.ack(tuple);
    }

    private class CalcluateGeThanBoltExpiredCallback implements CalculateCache.ExpiredCallback{

        public void expire(AtomicInteger currentValue) {
            int finalResult = currentValue.getAndSet(0);
            System.out.printf("%s的%d分钟内数据GE个数为%d; %s\n", new Object[]{desc, timeSection, finalResult, System.currentTimeMillis()/1000});
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}