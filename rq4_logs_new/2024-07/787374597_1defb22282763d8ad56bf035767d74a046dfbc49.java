package com.example.moreveiw.domain.project.model.dto.response;

import com.example.moreveiw.domain.image.model.dao.Image;
import com.example.moreveiw.domain.shape.circle.model.dao.Circle;
import com.example.moreveiw.domain.shape.line.model.dao.Line;
import com.example.moreveiw.domain.shape.rectangle.model.dao.Rectangle;
import com.example.moreveiw.domain.text.model.dao.Text;
import com.example.moreveiw.domain.threeD.model.dao.ThreeD;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ObjectResponse {
    private List<Image> images;
    private List<Circle> circles;
    private List<Line> lines;
    private List<Rectangle> rectangles;
    private List<Text> texts;
    private List<ThreeD> threeDs;
}