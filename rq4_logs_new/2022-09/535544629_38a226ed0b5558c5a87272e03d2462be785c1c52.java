package consumerdemo;

import java.util.function.Consumer;

public class Demo1 {

    public static void main(String[] args){

        Consumer<String> c = s->System.out.println(s);
        c.accept("shanid");
    }


}