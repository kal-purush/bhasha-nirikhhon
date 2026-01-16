package com.minisun.board.global.dto;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Data2<T,U> {
    private T data;
    private U data2;

    public Data2(T data, U data2){
        this.data = data;
        this.data2 = data2;
    }
}