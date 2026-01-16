package com.tazedaily.TAZEDaily.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.tazedaily.TAZEDaily.Domain.Channel;
import com.tazedaily.TAZEDaily.Repository.ChannelRepository;

@RestController
@RequestMapping("/channel")
public class ChannelController {

    @Autowired
    ChannelRepository channelRepository;

    @GetMapping
    public ResponseEntity<Iterable<Channel>> getAllChannels() {
        return new ResponseEntity<>(channelRepository.findAll(), HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<Channel> createChannel(@RequestBody Channel channel) {
        return new ResponseEntity<>(channelRepository.save(channel), HttpStatus.CREATED);
    }
    // @GetMapping("/{userId}")
    // public ResponseEntity<Iterable<Channel>> getUserChannels(@PathVariable Long
    // userId) {
    // return new ResponseEntity<>(channelRepository.findByUserId(userId),
    // HttpStatus.OK);
    // }
}