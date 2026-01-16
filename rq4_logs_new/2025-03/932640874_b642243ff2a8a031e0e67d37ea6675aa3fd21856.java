package dev.frilly.locket.room;

import androidx.room.Database;
import androidx.room.RoomDatabase;

import dev.frilly.locket.room.dao.UserProfileDao;
import dev.frilly.locket.room.entities.UserProfile;

/**
 * The main entrypoint of the SQLite database, this extends from Room
 * to allow our access to DAOs.
 */
@Database(entities = {UserProfile.class}, version = 1)
public abstract class LocalDatabase extends RoomDatabase {

    /**
     * Retrieves the shared instance of DAO for user profiles. User profiles
     * contain information about the user's friends, not for the user themself.
     *
     * @return the shared DAO instance
     */
    public abstract UserProfileDao userProfileDao();

}