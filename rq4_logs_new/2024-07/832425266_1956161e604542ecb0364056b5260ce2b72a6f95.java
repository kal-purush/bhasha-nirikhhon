package com.orange.ai.job;

import com.alibaba.fastjson.JSON;
import com.orange.ai.ai.model.IKimi;
import com.orange.ai.zsxq.model.IZsxqApi;
import com.orange.ai.zsxq.model.aggregates.UnAnsweredQuestionsAggregates;
import com.orange.ai.zsxq.model.vo.Topics;

import org.apache.hc.core5.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: orange
 * Date: 2024/7/25
 * Time: 10:25
 * Description:
 */


@EnableScheduling
@Configuration
public class ChatbotTask {
    private Logger logger = LoggerFactory.getLogger(ChatbotTask.class);

    @Value("${chatbot.groupId}")
    private String groupId;

    @Value("${chatbot.cookie}")
    private String cookie;

    @Resource
    private IZsxqApi iZsxqApi;

    @Resource
    private IKimi iKimi;


    @Scheduled(cron = "0/5 * * * * ?")
    public void run() {
        try {

            if (new Random().nextBoolean()) {
                logger.info("随机打样中");
                return;
            }

            GregorianCalendar gregorianCalendar = new GregorianCalendar();
            int hour = gregorianCalendar.get(Calendar.HOUR_OF_DAY);
            if (hour > 22 || hour < 7) {
                logger.info("ai下班了，不工作了");
                return;
            }

            //检索问题
            UnAnsweredQuestionsAggregates unAnsweredQuestionsAggregates = iZsxqApi.queryAnsweredQuestionsTopicId(groupId, cookie);
            logger.info("查询的问题为：{}", JSON.toJSON(unAnsweredQuestionsAggregates));
            List<Topics> topics = unAnsweredQuestionsAggregates.getResp_data().getTopics();
            if (null == topics || topics.isEmpty()) {
                logger.info("本次检索未查询到待会答问题");
                return;
            }

            for (Topics topic : topics) {
                String answer = iKimi.doChatGPT(topic.getTalk().getText());
                boolean status = iZsxqApi.answer(groupId, cookie, topic.getTopic_id(), answer);
                logger.info("编号 {} 问题 {} 回答 {} 状态 {}", topic.getTopic_id(), topic.getTalk().getText(), answer, status);

            }
            //ai回答



            //提交答案
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }
}