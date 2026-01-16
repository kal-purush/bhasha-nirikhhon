package com.transteven.multiplaylist.content.dailymotion.repository;


import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.transteven.multiplaylist.content.dailymotion.model.DailymotionModel;

/**
 * DailymotionRepository uses JpaRepository methods to:
 * 
 * Recieve, Update and Delete: Youtube Playlist data from the database.
 *@param DailymotionModel object containing a String list containing data on all videos in a playlist.
 *@param Long the primary key.
 */
@Repository
public interface DailymotionRepository extends JpaRepository<DailymotionModel, Long> {
}