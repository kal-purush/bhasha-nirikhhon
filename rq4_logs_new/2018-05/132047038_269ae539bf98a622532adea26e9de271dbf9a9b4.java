package com.spitfire.forexmemo.dao;

import com.spitfire.forexmemo.domain.NewsPiece;

import java.util.List;

public interface CustomFind {
    List<NewsPiece> findByDate(String startTime, String endTime);
}