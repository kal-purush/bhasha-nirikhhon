package test;

import bean.User;
import com.lpmas.framework.util.NumberKit;
import com.lpmas.framework.util.NumeralOperationKit;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

public class test {


    @Test
    public void testListStream(){

        User user = new User("1","张三","男");
        User user1 = new User("2","lisi","");
        User user3 = new User("2","zhaoyan","");

        List<User> userList = new ArrayList<>();
        userList.add(user);
        userList.add(user1);
        userList.add(user3);

        //取字段
        System.out.println(userList.stream().map(itme -> itme.getName()).collect(Collectors.toList()));
        //过滤对象
        System.out.println(userList.stream().filter(item -> item.getId().equals("1")).collect(Collectors.toList()));
//        System.out.println((String)userList.stream().collect(Collectors.joining()));

        //List 转 Map
        Map<String,String> map = userList.stream().collect(Collectors.toMap(User::getId,User::getName,(key1, key2) -> key1));
        System.out.println(map);

        //排序
        List<String> collect = userList.stream().sorted(Comparator.comparing(User::getId).reversed()).map(itme -> itme.getId()).collect(Collectors.toList());
        System.out.println(collect);

    }

    @Test
    public void testList(){
        List<Integer> list = new ArrayList<>();
        list.add(1);
        System.out.println(list);
        list.set(0,2);
//        list.
        System.out.println(list);
    }

    @Test
    public void test(){
        String a = "ss";
        int i = (int) Double.parseDouble(a);
        System.out.println(i);
    }

    @Test
    public void test2(){
        String a = "123";
        String b = "1234";
        int c = 1;
        int d =  1;
        System.out.println(a == b);
        System.out.println(c == d );
    }

    @Test
    public void test3(){
        List<Integer> list = new ArrayList<>();
        list.add(2);
        list.add(2);
        list.add(3);
        list.add(4);

        Iterator<Integer> it = list.iterator();
        while (it.hasNext()) {
            int i = it.next();
            if (i %2 == 0) {
                it.remove();
            }
        }
//        HashMap
        System.out.println(list.toString());
    }

    @Test
    public void test4(){
        List<Integer> list = new ArrayList<>();
        list.add(2);//0
        list.add(2);//1
        list.add(3);//2
        list.add(4);//3

//       int index= 2;
//        System.out.println(list.get(index--));
//        System.out.println(list.get(--index));

        System.out.println(list);
        Set<Integer> set = new HashSet<>(list);
        System.out.println(set);
    }

    public int tt(){
        try {
            return 1;
        } catch (Exception e) {
            return 2;
        } finally {
            return 3;
        }
    }

    @Test
    public void test5(){
        System.out.println(tt());
        User u = new User();
        u.hashCode();
    }

    @Test
    public void test6(){
        int n = 10 - 1;
        //1001 >>  1 = 0100 | 1001 = 1101 = 13 = n
        n |= n >>> 1;
        System.out.println(n);
        n |= n >>> 2;
        System.out.println(n);
        n |= n >>> 4;
        System.out.println(n);
        n |= n >>> 8;
        System.out.println(n);
        n |= n >>> 16;
        System.out.println(n);
    }


    class Node {
        private int id;
        private List<Node> chil;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public List<Node> getChil() {
            return chil;
        }

        public void setChil(List<Node> chil) {
            this.chil = chil;
        }

        public Node(int id, List<Node> chil) {
            this.id = id;
            this.chil = chil;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "id=" + id +
                    ", chil=" + chil +
                    '}';
        }
    }

    private Node find(Node n,int i) {
        if (n.getId() == i) {
            return n;
        }
        if (n.getChil() != null) {
            for (Node node1 : n.getChil()) {
                Node node2 = find(node1, i);
                if (node2 != null) {
                    return node2;
                }
            }
        }
        return null;
    }

    @Test
    public void test7(){

        List<Node> nodes4 = new ArrayList<>();
        nodes4.add(new Node(137, null));
        nodes4.add(new Node(15, null));
        nodes4.add(new Node(16, null));

        List<Node> nodes2 = new ArrayList<>();
        nodes2.add(new Node(7, null));
        nodes2.add(new Node(8, null));
        nodes2.add(new Node(9, nodes4));

        List<Node> nodes3 = new ArrayList<>();
        nodes3.add(new Node(10, null));
        nodes3.add(new Node(11, null));
        nodes3.add(new Node(12, null));

        List<Node> nodes1 = new ArrayList<>();
        nodes1.add(new Node(3, null));
        nodes1.add(new Node(4, nodes2));
        nodes1.add(new Node(5, null));
        nodes1.add(new Node(6, nodes3));
        Node node = new Node(1, nodes1);

        System.out.println(find(node,9));

    }

    public int ex(){
        int x= 1;
        try{
            return x;
        } finally {
            x= 2;
        }
    }

    @Test
    public void test8(){
        System.out.println(ex());
    }


    @Test
    public void test9(){
        double a = 0.9;
        double b = 0.8;
        double c = 0.7;
        System.out.println((a-b));
        System.out.println( (b-c));

    }
}