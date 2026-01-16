package com.tresmigos.fullstackvideo.controller;

import com.amazonaws.services.s3.model.ObjectListing;
import com.tresmigos.fullstackvideo.model.Video;
import com.tresmigos.fullstackvideo.service.AwsServiceClient;
import com.tresmigos.fullstackvideo.service.VideoService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VideoControllerTest {
    @InjectMocks
    VideoController videoController;

    @Mock
    VideoService service;
    @Mock
    AwsServiceClient awsServiceClient;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void getVideoURL() {
    }

    @Test
    void read() {
        //given
        Video video=new Video();
        ResponseEntity<Video>expected=new ResponseEntity<>(video,HttpStatus.OK);
        //when
        Mockito.doReturn(video).when(service).read(Mockito.anyLong());
        ResponseEntity<Video>actual=videoController.read(Mockito.anyLong());
        //then
        assertEquals(expected,actual);
    }

    @Test
    void updateVideoById() {
    }

    @Test
    void deleteVideoById() {
    }
}