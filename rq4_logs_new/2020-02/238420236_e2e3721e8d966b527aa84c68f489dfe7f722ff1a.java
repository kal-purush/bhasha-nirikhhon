/*
package com.wj.jpademo;

import com.wj.jpademo.model.Coffee;
import com.wj.jpademo.model.CoffeeOrder;
import com.wj.jpademo.model.OrderState;
import com.wj.jpademo.repository.CoffeeOrderRepository;
import com.wj.jpademo.repository.CoffeeRepository;
import com.wj.jpademo.service.CoffeeOrderService;
import com.wj.jpademo.service.CoffeeService;
import io.lettuce.core.ReadFrom;
import lombok.extern.slf4j.Slf4j;
import org.joda.money.CurrencyUnit;
import org.joda.money.Money;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.*;
import java.util.stream.Collectors;

@SpringBootApplication
@EnableJpaRepositories //开启JPA的注解
@Slf4j
@EnableCaching(proxyTargetClass = true) //开启SpringBoot缓存，基于类的拦截
public class JpaDemoApplication implements ApplicationRunner{

	 @Autowired
	 private CoffeeRepository coffeeRepository;

	 @Autowired
	 private CoffeeOrderRepository orderRepository;

	@Autowired
	private CoffeeService coffeeService;

	@Autowired
	private CoffeeOrderService orderService;

	*/
/*@Autowired
	private JedisPool jedisPool;

	@Autowired
	private JedisPoolConfig jedisPoolConfig;*//*


	@Bean
	public RedisTemplate<String,Coffee> redisTemplate(RedisConnectionFactory redisConnectionFactory){
          RedisTemplate<String,Coffee> templates = new RedisTemplate<>();
          templates.setConnectionFactory(redisConnectionFactory);
          return templates;
	}

	@Bean
	public LettuceClientConfigurationBuilderCustomizer customizer(){
		return builder ->builder.readFrom(ReadFrom.MASTER_PREFERRED);
	}

*/
/*	@Bean
	@ConfigurationProperties("redis")
	public JedisPoolConfig jedisPoolConfig(){
		return new JedisPoolConfig();
	}

	@Bean(destroyMethod = "close")
	public JedisPool jedisPool(@Value("${redis.host}") String host){
		log.info("host:{}",host);
 		return new JedisPool(host);
	}*//*


	@Override
	//@Transactional
	public void run(ApplicationArguments args) throws Exception {
		Optional<Coffee> mocha = coffeeService.findOneCoffee("mocha"); //Optional作为一个容器存放对象，可以减少空指针异常。
         log.info("coffee:{}",mocha);

		for (int i = 0; i <5 ; i++) {
			mocha = coffeeService.findOneCoffee("mocha");
		}
		log.info("Value from redis.....{}",mocha);


	}

	private void testCacheSpringAndRedis() throws InterruptedException {
		log.info("count:{}",coffeeService.findAllCoffee().size());
		for (int i = 0; i <10; i++) {
			log.info("Reading from cache....");
			coffeeService.findAllCoffee();
		}
		//coffeeService.reload();
		Thread.sleep(5000);
		log.info("Reading after reload....");

		coffeeService.findAllCoffee()
		.forEach(coffee -> log.info("Coffee:{}",coffee));
	}

	*/
/*private void testJedis() {
		log.info("jedisPoolConfig:{}",jedisPoolConfig.toString());
		try(Jedis jedis = jedisPool.getResource()) {
          coffeeRepository.findAll().forEach(coffee ->
		  jedis.hset("springbuks-menu",coffee.getName(),Long.toString(coffee.getPrice().getAmountMajorInt())));
			Map<String,String> map = jedis.hgetAll("springbuks-menu");
			log.info("menu:{}",map);
			String price = jedis.hget("springbuks-menu","espresso");
			log.info("the price of espresso is :{}", Money.of(CurrencyUnit.of("CNY"),Long.parseLong(price)));
		}
	}*//*


	private void updateOrder() {
		log.info("All coffees",coffeeRepository.findAll());
		Optional<Coffee> latte = coffeeService.findOneCoffee("Latte"); //Optional作为一个容器存放对象，可以减少空指针异常。
		if (latte.isPresent()){ //表示对象不为空
			CoffeeOrder order = orderService.createOder("wangjie",latte.get());
			log.info("update init to paid:{}",orderService.updateOrder(order, OrderState.PAID));
			log.info("update paid to init:{}",orderService.updateOrder(order,OrderState.INIT));

		}
	}

	private void initOrders() {
		Coffee espresso = Coffee.builder().name("espresso")
				.price(Money.of(CurrencyUnit.of("CNY"), 20.0))
				.build();
		coffeeRepository.save(espresso);
		log.info("Coffee: {}", espresso);

		Coffee latte = Coffee.builder().name("latte")
				.price(Money.of(CurrencyUnit.of("CNY"),30.0))
				.build();
		coffeeRepository.save(latte);
		log.info("Coffee: {}", latte);

		CoffeeOrder order = CoffeeOrder.builder()
				.customer("Li Lei")
				.items(Collections.singletonList(espresso))
				.state(OrderState.INIT)
				.build();
		orderRepository.save(order);
		log.info("Order: {}", order);

		order = CoffeeOrder.builder()
				.customer("Li Lei")
				.items(Arrays.asList(espresso, latte))
				.state(OrderState.INIT)
				.build();
		orderRepository.save(order);
		log.info("Order: {}", order);
	}
	private void findOrders(){
		//1-
		coffeeRepository.findAll(Sort.by(Sort.Direction.DESC,"id"))
		.forEach(o ->log.info("loading,{}",o));
		//2-
	*/
/*	List<CoffeeOrder> list = orderRepository.findTop3ByOrderByUpdateTimeDescIdAsc();
        log.info("findTop3ByOrderByUpdateTimeDescIdAsc:{}",getJoninedOrderId(list));
		//不开启事务，会报错
        list.forEach(o->{
			log.info("Order:{}",o.getId());
			o.getItems().forEach(i ->log.info("Item,{}",i));
		});
        //3-
		list = orderRepository.findByItems_Name("latte");
		log.info("findByItems_Name,{}",getJoninedOrderId(list));*//*



	}
	private String getJoninedOrderId(List<CoffeeOrder> list){
		return list.stream().map(o ->o.getId().toString())
				.collect(Collectors.joining(","));
	}
}
*/