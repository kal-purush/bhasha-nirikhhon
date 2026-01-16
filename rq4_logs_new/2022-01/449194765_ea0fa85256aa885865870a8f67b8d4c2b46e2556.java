package com.delphine.springbootquickstart.courseapidata.controller;


import com.delphine.springbootquickstart.courseapidata.model.Topic;
import com.delphine.springbootquickstart.courseapidata.service.TopicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class TopicController {

    @Autowired    //means needs dependency injection
    private TopicService topicService;

    @RequestMapping("/topics")
    public List<Topic> getAllTopics(){

        return topicService.getAllTopics();
    }

    //get topic by id
    @RequestMapping("/topics/{id}")
    public  Topic getTopic(@PathVariable String id){ //id is available in the path

        return topicService.getTopic(id);
    }

    //create a new topic
    @RequestMapping(method = RequestMethod.POST, value = "/topics")
    public void addTopic(@RequestBody Topic topic){

        topicService.addTopic(topic);
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/topics/{id}")
    public void updateTopic(@RequestBody Topic topic, @PathVariable String id){

        topicService.updateTopic(id, topic);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/topics/{id}")
    public void deleteTopic(@PathVariable String id){

        topicService.deleteTopic(id);
    }

}