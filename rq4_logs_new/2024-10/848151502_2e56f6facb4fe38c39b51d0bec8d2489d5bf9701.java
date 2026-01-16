package com.lucky.around.meal.datapipeline;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.lucky.around.meal.entity.Restaurant;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class RestaurantJdbcRepository {

  private final JdbcTemplate jdbcTemplate;

  public void saveAll(List<Restaurant> restaurantList) {
    String sql =
        "INSERT INTO restaurant "
            + "(id, restaurant_name, dosi, sigungu, category, rating_average) "
            + "VALUES (?, ?, ?, ?, ?, ?)";

    jdbcTemplate.batchUpdate(
        sql,
        new BatchPreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps, int i) throws SQLException {
            Restaurant restaurant = restaurantList.get(i);
            ps.setString(1, restaurant.getId());
            ps.setString(2, restaurant.getRestaurantName());
            ps.setString(3, restaurant.getDosi());
            ps.setString(4, restaurant.getSigungu());
            ps.setString(5, String.valueOf(restaurant.getCategory()));
            ps.setDouble(6, 0);
          }

          @Override
          public int getBatchSize() {
            return restaurantList.size();
          }
        });
  }
}