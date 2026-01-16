package src.my.com.app.stack;

import java.util.*;
import java.util.stream.Collectors;

public class Solution {
    public String removeDuplicates(String s) {
        
        // Iterative approach with stack

        Stack<Character> st = new Stack<>();

        for ( char c : s.toCharArray()){
            if ( st.size()!= 0){
                // compare with curr stack top
                if ( c != st.peek()){
                    st.push(c);
                } else {
                    st.pop();
                }
            } else {
                // insert
                st.push(c);
            }
        }

        if (st.size() == 0)
            return  "" ;

       return st.stream().map(String::valueOf).collect(Collectors.joining());
    }
}