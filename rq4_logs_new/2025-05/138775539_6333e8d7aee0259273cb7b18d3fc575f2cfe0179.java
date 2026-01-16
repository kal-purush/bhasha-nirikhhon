package my.com.app;

import java.util.*;


class Solution {

    // reverse the linedList and then perform addition
    // check for remaining carry at the end 
    public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        
        l1 = reverse(l1);
        l2 = reverse(l2);
       
        
        ListNode res = null;
        int car =0,  sum =0;
        while ( l1!=null || l2 !=null) {
            
            if(l1 == null){
                sum = 0 + l2.val;
            }
            else if(l2== null){
                sum = l1.val + 0 ;
            }
            
            else{
                sum = l1.val + l2.val;
            }

            sum = sum + car ;
            if ( sum >= 10 ){
                car = sum/10;
                sum = sum % 10;
            } else {
                car =0;
            }
            
            ListNode nt = new ListNode(sum);
            nt.next = res;
            res = nt;

            l1 = l1!=null? l1.next : null;
            l2 = l2!=null? l2.next : null;
        }
        
        if(car>0){
            ListNode nt = new ListNode(car);
            nt.next = res;
            res = nt;
        }
        
        return res;
    }
    
    public ListNode reverse (ListNode l) {
        
        ListNode prev = null ,curr = l , nex = l;
        
        while ( curr!=null ) {
            
            nex = curr.next;
            curr.next = prev;
            prev = curr;
            curr = nex;
            
        }
       return prev; 
    }
}