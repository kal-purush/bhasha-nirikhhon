package com.csst.hotelCrawler.dao;

import java.util.List;

import com.csst.hotelCrawler.entity.CommentStatisticBreakfast;

public interface CommentStatisticBreakfastDao {
    public List<CommentStatisticBreakfast> queryBreakfastComment(int id);
}