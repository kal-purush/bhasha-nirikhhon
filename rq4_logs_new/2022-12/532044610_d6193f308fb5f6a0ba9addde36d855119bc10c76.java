package ru.mirea.task22.task1;

import java.util.Stack;

public class Calc {

    public static void main(String[] args) throws esexep {
        try {
            System.out.println(conv("1 6 +"));
            System.out.println(conv("1 9 / 4 0 * -"));
            System.out.println(conv("5 0 /"));

        } catch(Exception e){
            System.out.println(e.getMessage());
        }
    }


    public static double conv(String in) throws esexep, divz {
        Double result = 0d;


        String now = "";
        Stack<Double> st_cur = new Stack<>();
        for(int i = 0; i < in.length(); i++){
            if (isOperand(((Character)in.charAt(i)).toString())){
                if (st_cur.size() < 2) throw new esexep();
                result = calcul(st_cur.pop(), st_cur.pop(), ((Character)in.charAt(i)).toString());
                st_cur.push(result);
            }
            else if (in.charAt(i) == ' ' && !now.replace(" ", "").equals("") && !now.replace(" ", "").equals(".")){

                st_cur.push(Double.parseDouble(now.replace(" ", "")));
                now = "";
            }
            else{
                now += in.charAt(i);
            }
        }
        if(st_cur.empty()){
            return 0;
        }else return st_cur.pop();
    }

    public static boolean isOperand(String str){
        return str.equals("+") || str.equals("-") || str.equals("*") || str.equals("/");
    }

    public static Double calcul(Double a, Double b, String o) throws divz {
        if (o.equals("/") && a == 0) throw new divz();
        switch(o){
            case "+" :
                return a + b;
            case "-" :
                return b - a;
            case "*" :
                return a * b;
            case "/" :
                return b / a;
        }
        return 0d;
    }
}