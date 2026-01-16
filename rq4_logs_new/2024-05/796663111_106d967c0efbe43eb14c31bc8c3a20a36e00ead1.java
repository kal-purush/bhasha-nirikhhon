package t3h.manga.mangaweb.controller;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import t3h.manga.mangaweb.crawler.MangaDetailCrawler;
import t3h.manga.mangaweb.crawler.TagCrawler;
import t3h.manga.mangaweb.entity.Author;
import t3h.manga.mangaweb.entity.Tag;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/crawler/tag")
public class TagCrawlerController {
    @GetMapping()
    public ResponseEntity tagCrawler() {

        SessionFactory sessionFactory = new Configuration().configure("hibernate.cfg.xml").buildSessionFactory();
        try (Session session = sessionFactory.openSession()) {
            session.beginTransaction();
            TagCrawler tagCrawler = new TagCrawler();
            ArrayList<String> tags = tagCrawler.tagAfterCrawler();
            for (String tag : tags) {
                Tag newTag = new Tag();
                newTag.setName(tag);
                session.save(newTag);
            }
            session.getTransaction().commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Đóng SessionFactory khi không cần sử dụng nữa
            sessionFactory.close();
        }
        return new ResponseEntity("Crawler done", HttpStatus.OK);
    }
}