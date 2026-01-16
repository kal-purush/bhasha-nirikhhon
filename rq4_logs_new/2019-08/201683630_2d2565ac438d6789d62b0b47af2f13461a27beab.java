package com.intouristing.intouristing.model.repository;

import com.intouristing.intouristing.model.entity.User;
import com.intouristing.intouristing.model.entity.UserPosition;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

/**
 * Created by Marcelo Lacroix on 18/08/2019.
 */
public interface UserPositionRepository extends JpaRepository<UserPosition, Long> {

    @Query(value = "select user from UserPosition userPosition " +
            "join userPosition.user user " +
            "where userPosition.latitude >= ?1 " +
            "and userPosition.latitude <= ?2 " +
            "and userPosition.longitude >= ?3 " +
            "and userPosition.longitude <= ?4 ")
    List<User> findAllUsersInRange(double minLat, double maxLat, double minLong, double maxLong);
}