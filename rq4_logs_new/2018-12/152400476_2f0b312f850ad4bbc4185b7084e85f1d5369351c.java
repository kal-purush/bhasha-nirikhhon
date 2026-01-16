package com.hydata.intelligence.platform.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hydata.intelligence.platform.model.MQTT;
import com.hydata.intelligence.platform.repositories.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import static com.hydata.intelligence.platform.service.MqttReceiveConfig.cachedThreadPool;

/**
 * @author: Jasmine
 * @createTime: 2018-12-27 14:49
 * @description: <MQTT接收消息处理，添加和删除订阅>
 * @modified:
 */
public class MqttHandler {

    @Autowired
    private DatastreamModelRepository datastreamModelRepository;
    @Autowired
    private ProductRepository productRepository;
    @Autowired
    private TriggerRepository triggerRepository;
    @Autowired
    private DeviceService deviceService;
    @Autowired
    private TriggerService triggerService;
    @Autowired
    private DeviceDatastreamRepository deviceDatastreamRepository;
    @Autowired
    private DdTriggerRepository ddTriggerRepository;
    @Autowired
    private TriggerTypeRepository triggerTypeRepository;

    private static int qos = 2;

    private static Logger logger = LogManager.getLogger(MqttReceiveConfig.class);

    /**
     * haizhe
     * 增加一个方法(供pyt调用）
     * 添加topic，传入sn进来，将其加为topic
     * @return
     */
    @Bean
    public static MessageChannel mqttDataChannel() {
        return new DirectChannel();
    }
    public static void mqttAddDevice(String topic) throws MqttException {
        //sendClient.subscribe(topic);
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(MQTT.getClientId()+"_inbound",  MqttReceiveConfig.mqttClientFactory());

        adapter.setCompletionTimeout(MQTT.getCompletionTimeout());
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(qos);
        adapter.setOutputChannel(mqttDataChannel());
        adapter.addTopic(topic);

    }


    /**
     * haizhe
     * 增加一个方法，（供pyt调用）
     * 删除topic，传入sn进来，将其topic去除，不再订阅
     * TODO
     * @return
     */
    public static void mqttRemoveDevice(String topic) throws MqttException{
        //sendClient.unsubscribe(topic);
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(MQTT.getClientId()+"_inbound",  MqttReceiveConfig.mqttClientFactory());

        adapter.setCompletionTimeout(MQTT.getCompletionTimeout());
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(qos);
        adapter.setOutputChannel(mqttDataChannel());
        adapter.removeTopic(topic);
    }


    //通过通道获取数据
    @Bean
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public MessageHandler handler() {
        return new MessageHandler() {
            @Override
            public void handleMessage(Message<?> message) throws MessagingException {
                String topic = message.getHeaders().get("deviceSn").toString();
                String content = message.getPayload().toString();
                cachedThreadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        //解析收到的实时数据流
                        JSONArray data= mqttDataAnalysis(content);
                        //存储实时数据流到mongodb
                        deviceService.dealWithData(topic, data);
                        //进行触发器判断
                        try {
                            triggerService.TriggerAlarm(topic,data);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                });
            }
        };
    }


    /**
     * MQTT数据解析
     * 实时信息流格式String：name1, value1; name2, value2;...
     * 返回格式JSONArray：[{"dm_name":"name1","value":"value1"},{"dm_name":"name2","value":"value2"},...]
     */
    public JSONArray mqttDataAnalysis(String data){
        //JSONArray result = JSONArray.parseArray(data);
        JSONObject object = new JSONObject();
        JSONArray result = new JSONArray();
        String[] datas = data.split(";") ;
        for (int i = 0; i<datas.length; i++){
            String[] tmp = datas[i].split(",");
            String dm_name = tmp[0];
            int value = Integer.parseInt(tmp[1]);
            object.put("dm_name", dm_name);
            object.put("value", value);
            result.add(object);
        }
        return result;
    }



}