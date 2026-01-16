package com.example.englishforkids;

import com.example.englishforkids.dao.VocabularyDAO;
import com.example.englishforkids.model.Lesson;
import com.example.englishforkids.model.Vocabulary;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;

import java.util.List;

public class VocabularyViewController {
    private Lesson lesson;
    private List<Vocabulary> lstVocabulary;
    private VocabularyDAO vocabularyDAO;
    @FXML
    private Label txtWord;
    @FXML
    private Label txtMean;
    @FXML
    private Label txtPhonetic;
    @FXML
    private Label txtSynonyms;
    @FXML
    private Label txtAntonyms;
    @FXML
    private Button btnPrevious;
    @FXML
    private Button btnNext;
    private int index;

    public VocabularyViewController() {
    }

    public void initialize(){
        index = 0;
        vocabularyDAO = new VocabularyDAO();
        lstVocabulary = vocabularyDAO.selectBySql(VocabularyDAO.SELECT_ALL_VOCABULARY_IN_LESSON_QUERY, lesson.getIdLesson());
        getAntonyms();
        getSynonyms();
        loadScene();
        btnPrevious.setOnAction(event -> {
            if(index >= 1){
                index--;
                loadScene();
            }
        });
        btnNext.setOnAction(event -> {
            if(index < lstVocabulary.size()-1){
                index++;
                loadScene();
            }
        });
    }

    public Lesson getLesson() {
        return lesson;
    }

    public void setLesson(Lesson lesson) {
        this.lesson = lesson;
    }

    private void getAntonyms(){
        for (Vocabulary vocabulary: lstVocabulary) {
            vocabulary.setAntonyms(vocabularyDAO.selectBySql(VocabularyDAO.SELECT_ALL_ANTONYMS_VOCABULARY_QUERY,
                    vocabulary.getIdVocabulary()));
        }
    }

    private void getSynonyms(){
        for (Vocabulary vocabulary: lstVocabulary) {
            vocabulary.setSynonyms(vocabularyDAO.selectBySql(VocabularyDAO.SELECT_ALL_SYNONYMS_VOCABULARY_QUERY,
                    vocabulary.getIdVocabulary()));
        }
    }

    private String getStringFromListVocabulary(List<Vocabulary> lst){
        StringBuilder stringBuilder = new StringBuilder();
        for (Vocabulary vocabulary: lst) {
            stringBuilder.append(vocabulary.getWord()).append(" ");
        }
        return stringBuilder.toString();
    }

    private void loadScene(){
        txtWord.setText(lstVocabulary.get(index).getWord());
        txtMean.setText(lstVocabulary.get(index).getMean());
        txtPhonetic.setText(lstVocabulary.get(index).getPhonetic());
        String antonyms = getStringFromListVocabulary(lstVocabulary.get(index).getAntonyms());
        String synonyms = getStringFromListVocabulary(lstVocabulary.get(index).getSynonyms());
        txtAntonyms.setText(antonyms);
        txtSynonyms.setText(synonyms);
    }


}