package com.company;

import com.company.model.Family;

/**
 * Created by Next on 19.11.2016.
 */
public interface FamilyTaskLogger {
    void log(String dateTime, String message);

    void log(Object o);
}