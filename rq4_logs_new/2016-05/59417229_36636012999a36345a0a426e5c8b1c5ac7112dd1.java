package ch.codebulb.delombok;

public class Client {
    private static final GetterSetterExample example = new GetterSetterExample();
    
    public static int getAge() {
        return example.getAge();
    }
}