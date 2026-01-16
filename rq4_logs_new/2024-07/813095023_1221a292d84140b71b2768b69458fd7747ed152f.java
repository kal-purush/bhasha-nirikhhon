package com.spring.mood.projectmvc.service;


import com.spring.mood.projectmvc.dto.requestDto.RequestMemberDto;
import com.spring.mood.projectmvc.entity.Member;
import com.spring.mood.projectmvc.entity.Topic;
import com.spring.mood.projectmvc.mapper.TopicMapper;
import com.spring.mood.projectmvc.repository.TopicRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class TopicService {
    private final TopicMapper topicMapper;

    public boolean saveTopic(String topicContent) {
        System.out.println("topicContent = " + topicContent);
        boolean save = topicMapper.save(topicContent);
        return save;
    }


    public List<Topic> getTopic() {
        List<Topic> all = topicMapper.findAll();
        return all;
    }
}