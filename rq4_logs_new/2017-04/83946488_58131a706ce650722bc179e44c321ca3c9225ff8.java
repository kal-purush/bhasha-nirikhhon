/**
 * @author waldo.wy
 * @date 2017/4/17
 */
package org.waldo.demo.spring;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.statemachine.config.EnableStateMachine;
import org.springframework.statemachine.config.EnumStateMachineConfigurerAdapter;
import org.springframework.statemachine.config.builders.StateMachineStateConfigurer;
import org.springframework.statemachine.config.builders.StateMachineTransitionConfigurer;
import org.waldo.demo.spring.constants.OrderEvent;
import org.waldo.demo.spring.constants.OrderStatus;

import java.util.EnumSet;

/**
 * 类OrderStatusMachineConfig描述：
 *
 * @author waldo.wangy 2017/4/17 13:32
 */
// 还可以通过Builder的方式进行创建
@Configuration
@ComponentScan("org.waldo.demo.spring.listener")
@EnableStateMachine
public class OrderStateMachineConfig extends EnumStateMachineConfigurerAdapter<OrderStatus, OrderEvent> {

    @Override
    public void configure(StateMachineStateConfigurer<OrderStatus, OrderEvent> states)
            throws Exception {
        states
                .withStates()
                .initial(OrderStatus.UNPAID)
                .states(EnumSet.allOf(OrderStatus.class));
    }

    @Override
    public void configure(StateMachineTransitionConfigurer<OrderStatus, OrderEvent> transitions)
            throws Exception {
        transitions
                .withExternal()
                .source(OrderStatus.UNPAID).target(OrderStatus.WAITING_FOR_SEND)
                .event(OrderEvent.PAY)
                .and().withExternal()
                .source(OrderStatus.WAITING_FOR_SEND).target(OrderStatus.WAITING_FOR_RECEIVE)
                .event(OrderEvent.SEND)
                .and().withExternal()
                .source(OrderStatus.WAITING_FOR_RECEIVE).target(OrderStatus.DONE)
        ;
    }

}