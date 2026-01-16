package dao;

/**
 * Created by Tom on 16/4/24.
 */

import com.google.inject.Inject;
import com.google.inject.Provider;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.Query;

import com.google.inject.persist.Transactional;
import models.Diary;
import models.Post;
import models.UserTable;
import models.UserTable;
public class DiaryDao {
    @Inject
    Provider<EntityManager> EntityManagerProvider;

    public List<Diary> getDiaryFromUsers(List<UserTable> userlist) {
        EntityManager em = EntityManagerProvider.get();
        List<Diary> result = new ArrayList<>();

        String strQuery = "SELECT x FROM Diary x WHERE user_id IN (";
        for (int i = 0; i < userlist.size(); i++) {
            strQuery += "'" + userlist.get(i).getId() + "'";
            if(i != userlist.size() - 1)
                strQuery += ", ";
        }
        strQuery += ") ORDER BY timestamp DESC";
        // Get all diary from user's relatives posts
        Query q = em.createQuery(strQuery);
        result = (List<Diary>) q.getResultList();

        return result;
    }
    @Transactional
    public Diary getDiaryFromSearchResult(Long diary_id) {
        List<Diary> result = new ArrayList<>();
        EntityManager em = EntityManagerProvider.get();
        Query q = em.createQuery("SELECT x FROM Diary x WHERE diary_id =:diaryid");
        q.setParameter("diaryid", diary_id);
        result = (List<Diary>) q.getResultList();
        if(result.size() ==1) {
            return result.get(0);
        }
        return null;
    }
    @Transactional
    public List<Diary> getDiaryFromKeyword(String content) {
        List<Diary> result = new ArrayList<>();
        EntityManager em = EntityManagerProvider.get();
        Query q = em.createQuery("SELECT x FROM Diary x WHERE content LIKE :keyword OR title LIKE :keyword");
        q.setParameter("keyword", "%" + content + "%");
        result = (List<Diary>) q.getResultList();
        //System.out.println("+++++++++++++++++++++++++++search result: " + result.size());
        return result;
    }

}