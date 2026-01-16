public class course {
    String[] course=new String[]{"c#","s/w testing","ml"};
    int[] marks=new int[]{34,38,46};
    public String[] getCourse() {
        return course;
    }
    public int[] getMarks() {
        return marks;
    }
    void setMarks(int m){
        if(m==34){
            System.out.print("c#: ");
        }
        if(m==38){
            System.out.print("s/w testing: ");
        }
    }
//    public static void main(String[] args){
//        course c=new course();
//        int m[]= c.getMarks();
//        for(int i:m){
//            if(i<40){
//                System.out.println(i);
//            }
//
//        }
//    }
}