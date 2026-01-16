package com.online.cinema.entity.cinemahall.service.impl;

import com.online.cinema.entity.cinemahall.dao.CinemaHallDao;
import com.online.cinema.entity.cinemahall.model.CinemaHall;
import com.online.cinema.entity.cinemahall.service.CinemaHallService;
import com.online.cinema.lib.Inject;
import com.online.cinema.lib.Service;
import java.util.List;

@Service
public class CinemaHallServiceImpl implements CinemaHallService {
    @Inject
    private CinemaHallDao cinemaHallDao;

    @Override
    public CinemaHall add(CinemaHall cinemaHall) {
        return cinemaHallDao.add(cinemaHall);
    }

    @Override
    public List<CinemaHall> getAll() {
        return cinemaHallDao.getAll();
    }
}