package com.example.moreveiw.domain.project.service;

import com.example.moreveiw.domain.image.repository.ImageRepository;
import com.example.moreveiw.domain.project.model.dto.response.ObjectResponse;
import com.example.moreveiw.domain.shape.circle.repository.CircleRepository;
import com.example.moreveiw.domain.shape.line.repository.LineRepository;
import com.example.moreveiw.domain.shape.rectangle.repository.RectangleRepository;
import com.example.moreveiw.domain.text.repository.TextRepository;
import com.example.moreveiw.domain.threeD.repository.ThreeDRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@RequiredArgsConstructor
@Service
public class ProjectsService {

    private final ImageRepository imageRepository;
    private final CircleRepository circleRepository;
    private final LineRepository lineRepository;
    private final RectangleRepository rectangleRepository;
    private final TextRepository textRepository;
    private final ThreeDRepository threeDRepository;

    @Transactional
    public ObjectResponse getProjectByObject(Long projectId) {
        return ObjectResponse.builder()
                .images(imageRepository.findByProjectId(projectId))
                .circles(circleRepository.findByProjectId(projectId))
                .lines(lineRepository.findByProjectId(projectId))
                .rectangles(rectangleRepository.findByProjectId(projectId))
                .texts(textRepository.findByProjectId(projectId))
                .threeDs(threeDRepository.findByProjectId(projectId))
                .build();
    }
}