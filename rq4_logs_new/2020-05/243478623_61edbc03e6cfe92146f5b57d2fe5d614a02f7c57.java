import java.lang.Exception;

interface A {
    public String implementMe();
}

interface AA {
	String s = "s";
}

public class C implements A, AA {
	String a = s;
	
	public String didntImplementYou() {
		return "sorry";
	}
}