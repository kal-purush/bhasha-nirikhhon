

public class SillyLib {
    public SillyLib(String hi){
    	System.out.println("proving that this constructor was called: " + hi);
    }

    public int fivePlusTwoIs() { return 5 + 2; }

    public static void main(String[] args) {
    	SillyLib s = new SillyLib("ya");
    }
}