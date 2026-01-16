package com.anghel.music_band_organizer.repository.user;

import com.anghel.music_band_organizer.models.entities.User;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class CustomUserRepositoryImpl implements CustomUserRepository{

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public List<User> findFilteredUser(Long userId, String firstName, String lastName, String userEmail, LocalDateTime userBirthday, String pastExperience) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<User> cq = cb.createQuery(User.class);

        Root<User> user = cq.from(User.class);
        List<Predicate> predicates = new ArrayList<>();

        if (userId != null) {
            predicates.add(cb.equal(user.get("id"), userId));
        }
        if (firstName != null) {
            predicates.add(cb.equal(user.get("firstName"), firstName));
        }
        if (lastName != null) {
            predicates.add(cb.equal(user.get("lastName"), lastName));
        }
        if (userBirthday != null) {
            predicates.add(cb.equal(user.get("birthday"), userBirthday));
        }
        if (userEmail != null) {
            predicates.add(cb.equal(user.get("email"), userEmail));
        }
// not working... but solved in the UserServiceImpl under getFilteredUsers
//        if (pastExperience != null) {
//            predicates.add(cb.isMember(pastExperience, user.get("pastExperience")));
//        }

        cq.where(predicates.toArray(new Predicate[0]));
        return entityManager.createQuery(cq).getResultList();
    }
}