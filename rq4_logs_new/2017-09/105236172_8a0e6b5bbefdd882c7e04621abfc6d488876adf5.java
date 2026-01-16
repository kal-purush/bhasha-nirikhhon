package com.rt.bc.eventcenter.consumer.bus;

import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.rt.bc.eventcenter.consumer.mgr.EventConsumerMgr;
import com.rt.bc.eventcenter.consumer.util.JsonUtil;
import com.rt.bc.eventcenter.itf.IEventConsumer;
import com.rt.bc.eventcenter.itf.IEventConsumerBus;
import com.rt.bc.eventcenter.util.EventNameUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by shenxy on 28/9/17.
 *
 * 接收事件中心的事件统一上报, 并分发到关注的consumer
 */
@Component
//@Service(cluster = "broadcast")
@org.springframework.stereotype.Service("EventConsumerBusImpl")
public class EventConsumerBusImpl implements IEventConsumerBus {
    private final static Logger logger = LoggerFactory.getLogger(EventConsumerBusImpl.class);

    @Autowired
    private EventConsumerMgr eventConsumerMgr;

    @Override
    public void onBusEvent(String eventName, String eventListJson) {
        logger.warn("EventConsumerBusImpl--onBusEvent--clsName:" + eventName + ", eventListJson" + eventListJson + ", Thread:" + Thread.currentThread().hashCode());

        Class cls = EventNameUtil.parseEventObjClass(eventName);

        List dataList = JsonUtil.parseArray(eventListJson, cls);
        List<IEventConsumer> consumerList = eventConsumerMgr.getConsumer(eventName);
        if (consumerList != null && !consumerList.isEmpty()) {
            for (IEventConsumer consumer : consumerList) {
                try {
                    //consumer的泛型指向cls, 并且eventDto的key和value的类型一致都为cls, 因此这里的回调实际上类型必然匹配
                    consumer.onEvent(dataList);
                }
                catch (Exception ex) {
                    logger.warn(ex);
                }
            }
        }
    }
}