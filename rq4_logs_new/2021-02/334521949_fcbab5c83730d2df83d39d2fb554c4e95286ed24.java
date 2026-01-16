package com.sweng894.GetVaccinated.schedule;

import java.time.DayOfWeek;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Week {
  public static List<Map<DayOfWeek, Integer>> weeksOfMonth(Integer year, Integer month) {
    var weeks = new ArrayList<Map<DayOfWeek, Integer>>(5);
    var week = new HashMap<DayOfWeek, Integer>(7);
    var m = YearMonth.of(year, month);
    var lastDay = m.lengthOfMonth();
    for (int i = 1; i <= lastDay; i++) {
      var dayOfWeek = m.atDay(i).getDayOfWeek();
      week.put(dayOfWeek, i);
      if (dayOfWeek == DayOfWeek.SATURDAY || i == lastDay) {
        weeks.add(week);
        week = new HashMap<>(7);
      }
    }
    return weeks;
  }
}