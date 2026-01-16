/*
 * Copyright (C) 2015 Baidu, Inc. All Rights Reserved.
 */
package demo.xmy.com.mp3.core.model;

/**
 * Created by xumengyang01 on 2015/2/11.
 */
public enum SingleIndex {

    PRE(1),NEXT(2);

    private int index;

    private SingleIndex(int i){
        index = i;
    }

    public int getValue(){
        return index;
    }
}