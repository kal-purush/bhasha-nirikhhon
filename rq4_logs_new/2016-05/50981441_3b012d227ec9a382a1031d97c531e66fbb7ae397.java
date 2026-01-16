package tyfrontier.cleanarchitecturesample.data.repository;

import javax.inject.Inject;

import rx.Observable;
import tyfrontier.cleanarchitecturesample.domain.model.Article;
import tyfrontier.cleanarchitecturesample.domain.net.WebApi;
import tyfrontier.cleanarchitecturesample.domain.repository.ArticleRepository;

public class ArticleRepositoryImpl implements ArticleRepository {

    private final WebApi webApi;

    @Inject
    public ArticleRepositoryImpl(WebApi webApi) {
        this.webApi = webApi;
    }

    @Override
    public Observable<Article> findArticles() {
        return webApi.getArticles(1, 20);
    }
}