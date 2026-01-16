package com.numo.api.global.comm.date;

import com.numo.domain.base.QBaseDate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;

public class DateCondition {

    public static BooleanExpression createDateCondition(QBaseDate qDate, DateRequestDto dateRequest) {
        BooleanExpression result = Expressions.asString("1").eq("1");
        Integer year = dateRequest.year();
        Integer month = dateRequest.month();
        Integer day = dateRequest.day();
        Integer week = dateRequest.week();

        if (year != null) {
            result = result.and(qDate.year.eq(year));
        }

        if (month != null) {
            result = result.and(qDate.month.eq(month));
        }

        if (day != null) {
            result = result.and(qDate.day.eq(day));
        }

        if (week != null) {
            result = result.and(qDate.week.eq(week));
        }

        return result;
    }
}