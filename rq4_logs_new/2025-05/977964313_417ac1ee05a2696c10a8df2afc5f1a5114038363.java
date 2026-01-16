package org.skypro.skyshop.model.article;

import org.skypro.skyshop.model.search.Searchable;

import java.util.Objects;
import java.util.UUID;

public class Article implements Searchable {
    public String nameArticle;
    public String contentArticle;
    public final UUID id;

    public Article(String nameArticle, String contentArticle, UUID id) {
        this.nameArticle = nameArticle;
        this.contentArticle = contentArticle;
        this.id = id;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public String toString() {
        return nameArticle + " " + contentArticle;
    }

    @Override
    public String searchableTerm() {
        return nameArticle + " " + contentArticle;
    }

    @Override
    public String searchableType() {
        return "ARTICLE";
    }

    @Override
    public String searchableName() {
        return nameArticle;
    }

    @Override
    public void getStringRepresentation() {
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Article article = (Article) o;
        return Objects.equals(nameArticle, article.nameArticle);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nameArticle);
    }
}