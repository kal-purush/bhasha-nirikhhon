package Domain;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.HashSet;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.mapping.Set;
import org.hibernate.type.EnumType;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

/**
 * A NewsArticle.
 */
@Entity
@Table(name = "news_article")
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
@SuppressWarnings("common-java:DuplicatedBlocks")
public class NewsArticle implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "title")
    private String title;

    @Column(name = "author")
    private String author;

    @Column(name = "article")
    private String article;

    @Column(name = "genre")
    private Genre genre;

    @Column(name = "date")
    private LocalDate date;

    @Column(name = "likes")
    private Integer likes;

    @OneToMany(mappedBy = "newsArticle")
    @Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
    @JsonIgnoreProperties(value = { "newsArticle", "user" }, allowSetters = true)
    private Set<Comment> Comments = new HashSet<>();

    // jhipster-needle-entity-add-field - JHipster will add fields here

    public Long getId() {
        return this.id;
    }

    public NewsArticle id(Long id) {
        this.setId(id);
        return this;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTitle() {
        return this.title;
    }

    public NewsArticle title(String title) {
        this.setTitle(title);
        return this;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAuthor() {
        return this.author;
    }

    public NewsArticle author(String author) {
        this.setAuthor(author);
        return this;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getArticle() {
        return this.article;
    }

    public NewsArticle article(String article) {
        this.setArticle(article);
        return this;
    }

    public void setArticle(String article) {
        this.article = article;
    }

    public Genre getGenre() {
        return this.genre;
    }

    public NewsArticle genre(Genre genre) {
        this.setGenre(genre);
        return this;
    }

    public void setGenre(Genre genre) {
        this.genre = genre;
    }

    public LocalDate getDate() {
        return this.date;
    }

    public NewsArticle date(LocalDate date) {
        this.setDate(date);
        return this;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public Integer getLikes() {
        return this.likes;
    }

    public NewsArticle likes(Integer likes) {
        this.setLikes(likes);
        return this;
    }

    public void setLikes(Integer likes) {
        this.likes = likes;
    }

    public Set<Comment> getComments() {
        return this.Comments;
    }

    public void setComments(Set<Comment> Comments) {
        if (this.Comments != null) {
            this.Comments.forEach(i -> i.setNewsArticle(null));
        }
        if (Comments != null) {
            Comments.forEach(i -> i.setNewsArticle(this));
        }
        this.Comments = Comments;
    }

    public NewsArticle Comments(Set<Comment> Comments) {
        this.setComments(Comments);
        return this;
    }

    public NewsArticle addComment(Comment Comment) {
        this.Comments.add(Comment);
        Comment.setNewsArticle(this);
        return this;
    }

    public NewsArticle removeComment(Comment Comment) {
        this.Comments.remove(Comment);
        Comment.setNewsArticle(null);
        return this;
    }

    // jhipster-needle-entity-add-getters-setters - JHipster will add getters and
    // setters here

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NewsArticle)) {
            return false;
        }
        return id != null && id.equals(((NewsArticle) o).id);
    }

    @Override
    public int hashCode() {
        // see
        // https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "NewsArticle{" +
                "id=" + getId() +
                ", title='" + getTitle() + "'" +
                ", author='" + getAuthor() + "'" +
                ", article='" + getArticle() + "'" +
                ", genre='" + getGenre() + "'" +
                ", date='" + getDate() + "'" +
                ", likes=" + getLikes() +
                "}";
    }
}