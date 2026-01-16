public class HelloImpl implements Hello {
    public final String version = "v1";
    private String mArg;

    public void _init(String arg) {
        mArg = arg;
    }

    public void hello() {
        System.out.println("Hello World " + version + ": " + mArg);
    }
}