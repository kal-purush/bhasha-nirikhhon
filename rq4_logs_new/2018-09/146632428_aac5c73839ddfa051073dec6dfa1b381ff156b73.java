package LinkedList;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class UseLinkList {
    public static void main(String[] args) {


        List<String> list=new LinkedList<>();
        list.add("NY");
        list.add("NJ");
        list.add("PA");
        for (String s:list){
            System.out.println(s);
        }
        System.out.println("Alternative way");
        Iterator it=list.iterator();
        while (it.hasNext()){
            System.out.println(it.next());
        }
    }
}