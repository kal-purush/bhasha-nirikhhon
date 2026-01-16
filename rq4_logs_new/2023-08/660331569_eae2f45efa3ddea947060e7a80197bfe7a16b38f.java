package com.cbfacademy.people;

public class ExtraPerson extends ImmutablePerson{

    protected String middleName;
    protected String nickName;

    public ExtraPerson(String firstName, String middleName, String lastName, String nickName){
    
        super(firstName, lastName);
        this.middleName = middleName;
        this.nickName = nickName;
    }
    
}