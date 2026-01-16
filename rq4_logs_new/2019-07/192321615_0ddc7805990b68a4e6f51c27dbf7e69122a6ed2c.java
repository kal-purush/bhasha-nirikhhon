package com.ixygj.myletter.demo;

import java.util.ArrayList;
import java.util.List;

public class LastIssueSubDemo {
    public static void main(String[] args) {
        String substr = "4505810";
        List<String> newsubstrArr = new ArrayList<String>();
        for(int j=2;j<7;j++){
            for(int i = 0;i<7;i++){
                if(i+j>7){
                    break;
                }else{
                    newsubstrArr.add(substr.substring(i,i+j));
                }
            }
        }
        //[45, 50, 05, 58, 81, 10, 450, 505, 058, 581, 810, 4505, 5058, 0581, 5810, 45058, 50581, 05810, 450581, 505810]
        String listStr = newsubstrArr.toString();
        System.out.println(listStr);
        StringBuffer stringBuffer = new StringBuffer();
        for(String sub:newsubstrArr){
            stringBuffer.append("\'").append(sub).append("\',");
        }
        //'45','50','05','58','81','10','450','505','058','581','810','4505','5058','0581','5810','45058','50581','05810','450581','505810',
        System.out.println(stringBuffer.toString());
    }
}