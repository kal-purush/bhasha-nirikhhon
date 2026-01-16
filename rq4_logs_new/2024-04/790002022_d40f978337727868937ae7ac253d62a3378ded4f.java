public class InterviewQuestion {
    public static void main(String args[]){
        boolean var1 = true;
        boolean var2 = false;

        System.out.println((var1 & var2));

        System.out.println("=====");

        char array[] = new char[10];

        for (int i=0; i< 10; ++i){
            array[i] ='i';
            System.out.println(array[i]+"");
            i++;
        }


        System.out.println("=====");

        //System.out.println(Level.BOTTOM.ordinal());

        Level level = Level.MEDIUM;
        System.out.println("=====");
        System.out.println(level);
    }


}