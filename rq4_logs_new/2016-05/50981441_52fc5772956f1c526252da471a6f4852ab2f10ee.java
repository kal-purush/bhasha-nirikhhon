package tyfrontier.cleanarchitecturesample.data.cache;

import android.support.annotation.NonNull;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;

import io.realm.RealmObject;
import tyfrontier.cleanarchitecturesample.domain.model.Article;

public class ArticleDto extends RealmObject {

    public String id;
    public String title;
    public Date createdAt;
    public String tags;
    public String url;
    public String userName;

    public Article map() {
        try {
            return new Article(
                    this.id,
                    this.title,
                    Arrays.asList(this.tags.split(",")),
                    this.userName,
                    this.createdAt,
                    new URL(this.url));
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void map(@NonNull Article article) {
        this.id = article.getId();
        this.title = article.getTitle();
        this.createdAt = article.getCreatedAt();
        this.url = article.getUrl().toString();
        this.userName = article.getUserName();

        StringBuilder stringBuilder = new StringBuilder();
        for (String tag : article.getTags()) {
            if (stringBuilder.length() > 0) {
                stringBuilder.append(",");
            }
            stringBuilder.append(tag);
        }
        this.tags = stringBuilder.toString();
    }
}