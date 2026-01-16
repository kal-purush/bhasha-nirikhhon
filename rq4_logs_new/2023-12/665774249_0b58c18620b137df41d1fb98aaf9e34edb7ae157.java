package com.example.demo.dto.workDto;

import com.example.demo.domain.Member;

public interface GetAllWork {

    String getTitle();
    String getImgUrl();
    String getContents();
    Member getMember(); //작품을 창작한 멤버의 이름
    Long getView();

    interface Member{
        String getId();
        String getName();
    }

}