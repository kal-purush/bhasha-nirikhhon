package jm.dao;

import jm.News;
import jm.api.dao.NewsDAO;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class NewsDaoImpl extends AbstractDAO<News> implements NewsDAO {

    @Override
    public Optional<News> getNewsByName(String name) {
        try {
            News news = (News) entityManager.createNativeQuery("SELECT * FROM news AS n WHERE n.name = :newsname")
                    .setParameter("newsname", name)
                    .getSingleResult();
            return Optional.of(news);
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}