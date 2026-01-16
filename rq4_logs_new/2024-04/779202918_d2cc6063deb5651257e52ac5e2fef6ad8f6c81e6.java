package com.example.englishforkids;

import com.example.englishforkids.model.Lesson;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.layout.Pane;

public class LessonViewController {
    private Lesson lesson;
    @FXML
    private Pane paneLesson;
    public void initialize(){
    }

    public Lesson getLesson() {
        return lesson;
    }

    public void setLesson(Lesson lesson) {
        this.lesson = lesson;
        Label lblTile = (Label) paneLesson.getChildren().get(0);
        lblTile.setText(lesson.getName());
    }
}