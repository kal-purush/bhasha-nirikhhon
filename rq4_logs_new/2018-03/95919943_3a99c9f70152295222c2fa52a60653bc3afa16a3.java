package com.stl.db.impl;

import com.stl.db.TravelerDB;
import com.stl.domain.TravelerDataRepository;
import com.stl.entity.Traveler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TravelerDBImpl implements TravelerDB {
    @Autowired
    private TravelerDataRepository travelerDataRepository;

    @Override
    public Traveler getByUsername(String username) {
        return travelerDataRepository.findByUsername(username);
    }

    @Override
    public void save(Traveler traveler) {
        travelerDataRepository.save(traveler);
    }
}