package com.fatepet.petrest;

import org.springframework.data.domain.Sort;

import java.util.Arrays;

public enum SortOption {

    DISTANCE("DISTANCE", null),
    RECOMMEND("RECOMMEND", Sort.by(Sort.Direction.ASC, "recommendRank")),
    POPULAR("POPULAR", Sort.by(Sort.Direction.DESC, "createdAt"));

    private final String key;
    private final Sort sort;

    SortOption(String key, Sort sort){
        this.key = key;
        this.sort = sort;
    }

    public static SortOption from(String key) {
        return Arrays.stream(values())
                .filter(opt -> opt.key.equalsIgnoreCase(key))
                .findFirst()
                .orElse(POPULAR);
    }

    public Sort getSort() {
        return sort;
    }

    public boolean isDistanceSort(){
        return this == DISTANCE;
    }

}