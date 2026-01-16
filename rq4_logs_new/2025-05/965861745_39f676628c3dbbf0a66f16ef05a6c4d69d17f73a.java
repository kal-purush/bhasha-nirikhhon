package com.daclink.citypulse.database;

import androidx.room.Dao;
import androidx.room.Insert;
import androidx.room.OnConflictStrategy;
import androidx.room.Query;

import com.daclink.citypulse.model.CachedEvent;
import com.daclink.citypulse.model.EventItem;

import java.util.List;
@Dao
public interface ActivitiesDAO {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    void insertAll(List<Activities> events);

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    void insert(Activities events);

    @Query("SELECT * FROM activities WHERE city = :city AND category = :category")
    List<Activities> getEventsByCityAndCategory(String city, String category);

    @Query("DELETE FROM activities WHERE city = :city AND category = :category")
    void deleteEventsForCityAndCategory(String city, String category);

    @Query("DELETE FROM activities WHERE api_id = :id")
    void deleteEvent(String id);

    @Query("DELETE FROM activities")
    void clearAll();

    @Query("SELECT * FROM activities WHERE wishlisted = :wishlisted")
    List<CachedEvent> getWishlistedEvent(boolean wishlisted);
    @Query("UPDATE activities SET wishlisted= :newWishlistStatus WHERE id = :id")
    void setWishlistEvent(String id, boolean newWishlistStatus);
}