import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/** 
* Created by IntelliJ IDEA. 
* User: zunzunwang 
* Date: 04/11/2015 
* 客户端测试，在客户端调用远程对象上的远程方法，并返回结果。 
*/ 
public class Client {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String serverAdress = "127.0.0.1";
		try {
			//在RMI服务注册表中查找名称为RHello的对象，并调用其上的方法
			Registry registry = LocateRegistry.getRegistry(serverAdress,18500);
			RemoteEcho echo = (RemoteEcho)registry.lookup("echo");
			System.out.println("Connection OK to "+serverAdress);
			String s = echo.Hello()+"\n"+echo.Echo("All your base are belong to us");
			System.out.println(s);
			}catch (RemoteException e) {
				e.printStackTrace();
			}catch (NotBoundException e) {
				e.printStackTrace();
			}

	}

}