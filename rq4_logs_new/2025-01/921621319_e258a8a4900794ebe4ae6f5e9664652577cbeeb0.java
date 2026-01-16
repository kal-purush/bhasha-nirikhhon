package com.metamong.mt.member.exception;

import java.util.NoSuchElementException;

import lombok.Getter;

@Getter
public class MemberNotFoundException extends NoSuchElementException {
    private final String userId;

    public MemberNotFoundException(String userId, String message, Throwable e) {
        super(message, e);
        this.userId = userId;
    }
    
    public MemberNotFoundException(String userId, String message) {
        super(message);
        this.userId = userId;
    }
    
    public MemberNotFoundException(String userId, Throwable e) {
        super(e);
        this.userId = userId;
    }
    
    public MemberNotFoundException(String userId) {
        super();
        this.userId = userId;
    }
}