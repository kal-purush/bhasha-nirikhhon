package com.example;

import lombok.Getter;
import lombok.Setter;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Nam on 10/11/2018
 */
@Getter
@Setter
public class Application {

    public static void main(String[] args) {
        try{
            ExecuteInfo executeInfo = new ExecuteInfo("q","a");
            new Thread(executeInfo).start();

        }catch(Exception e){ System.out.println(e);}
    }


}