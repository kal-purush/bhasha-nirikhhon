package com.joymeter.utils;

import com.joymeter.dto.ActivityDTO;

import java.util.Comparator;

/**
 * Created by hramovecchi on 30/11/2015.
 */
public class ActivityComparator implements Comparator<ActivityDTO> {
    @Override
    public int compare(ActivityDTO a1, ActivityDTO a2) {
        if (a1.getStartDate() < a2.getStartDate()) {
            return 1;
        }
        return -1;
    }
}