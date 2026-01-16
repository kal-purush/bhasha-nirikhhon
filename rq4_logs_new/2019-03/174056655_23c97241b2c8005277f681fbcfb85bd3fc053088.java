package cn.sanleny.configclient.sample;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

/**
 * 监听到事件
 * @Author: sanleny
 * @Date: 2019-03-29
 * @Description: cn.sanleny.configclient.sample
 * @Version: 1.0
 */
@Configuration
public class EventConfiguration {

    /**
     * 监听 {@link MyApplicationEvent}
     *
     * @param event {@MyApplicationEvent}
     */
    @EventListener
    public void onEvent(MyApplicationEvent event) {
        System.out.println("监听到事件：" + event);
        //各种实现
    }

}