package xyz.ivyxjc.pubg4j.web;

import com.alibaba.druid.support.http.StatViewServlet;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import xyz.ivyxjc.pubg4j.web.httpclient.GetApi;
import xyz.ivyxjc.pubg4j.web.messages.MessagesProducer;

@Slf4j
@EnableAspectJAutoProxy
@SpringBootApplication
@EnableTransactionManagement
@MapperScan("xyz.ivyxjc.pubg4j.web.dao")
@PropertySource(value = {"classpath:application.yaml", "classpath:application-private.yaml"})
public class Pubg4jApplication {

    @Autowired
    private GetApi mGetApi;

    @Value("${druid_user}")
    private String druidUsername;

    @Value("${druid_password}")
    private String druidPassword;

    @Autowired
    private MessagesProducer mMessagesProducer;

    public static void main(String[] args) {
        System.setProperty("java.security.auth.login.config", "classpath:kafka_client_jaas.conf");
        System.setProperty("java.specification.version", "1.8");
        ConfigurableApplicationContext context =
            SpringApplication.run(Pubg4jApplication.class, args);

        //MessagesProducer mp=context.getBean(MessagesProducer.class);
        //for (int i = 0; i < 3; i++) {
        //    //调用消息发送类中的消息发送方法
        //    log.info("hello------");
        //    //mp.send("hello kafka");
        //    try {
        //        Thread.sleep(3000);
        //    } catch (InterruptedException e) {
        //        e.printStackTrace();
        //    }
        //}
    }

    //@Bean
    //public CommandLineRunner doSome() {
    //    return t -> {
    //        PubgPlayer player=mGetApi.filterPlayerName("pc-as", "Snaketc_mozz");
    //        mPubgPlayerRepoServiceImpl.insertPubgPlayer(player);
    //        mGetApi.filterMatchId("pc-as", "94cb39be-983b-491b-aadf-bc781876cda6");
    //    };
    //}

    //@Bean
    //public CommandLineRunner sendMessage(){
    //    return t->{
    //        mMessagesProducer.send("Hello Kafka!");
    //    };
    //}

    @Bean
    public RequestConfig requestConfig() {
        return RequestConfig.custom()
            .setSocketTimeout(10000)
            .setConnectTimeout(10000)
            .setConnectionRequestTimeout(10000)
            .build();
    }

    @Bean
    public CloseableHttpClient httpClient(RequestConfig requestConfig) {
        PoolingHttpClientConnectionManager connectionManager =
            new PoolingHttpClientConnectionManager();

        return HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
    }

    @Bean
    public ServletRegistrationBean druidServlet() {
        ServletRegistrationBean servletRegistrationBean = new ServletRegistrationBean();
        servletRegistrationBean.setServlet(new StatViewServlet());
        servletRegistrationBean.addUrlMappings("/druid/*");
        Map<String, String> initParameters = new HashMap<>();
        initParameters.put("resetEnable", "false"); //禁用HTML页面上的“Rest All”功能
        initParameters.put("allow", "");  //ip白名单（没有配置或者为空，则允许所有访问）
        initParameters.put("deny", ""); //ip黑名单
        initParameters.put("loginUsername", druidUsername);  //++监控页面登录用户名
        initParameters.put("loginPassword", druidPassword);  //++监控页面登录用户密码
        servletRegistrationBean.setInitParameters(initParameters);
        return servletRegistrationBean;
    }
}