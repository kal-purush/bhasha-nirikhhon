package com.triplexng.taskmanagement.persistence.model;

import java.util.Objects;

public class ArticleContent {

    int numSentences;
    String title;
    String content;

    public ArticleContent(int numSentences, String title, String content) {
        this.numSentences = numSentences;
        this.title = title;
        this.content = content;
    }

    public int getNumSentences() {
        return numSentences;
    }

    public void setNumSentences(int numSentences) {
        this.numSentences = numSentences;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArticleContent that = (ArticleContent) o;
        return numSentences == that.numSentences &&
                title.equals(that.title) &&
                content.equals(that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numSentences, title, content);
    }

    @Override
    public String toString() {
        return "ArticleContent{" +
                "numSentences=" + numSentences +
                ", title='" + title + '\'' +
                ", content='" + content + '\'' +
                '}';
    }

}
