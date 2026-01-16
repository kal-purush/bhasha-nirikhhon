import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;


public class Comunicacion extends Thread {
	
	private Socket s;
	
	public Comunicacion() {
		try {
			s =  new Socket(InetAddress.getByName("127.0.0.1"), 6001);			
			recibir();			
		} catch (IOException e) {		
			e.printStackTrace();
		}
		
	}

	private void recibir() throws IOException {
		byte[] buf =  new byte[512];
		InputStream entrada = s.getInputStream();		
		ObjectInputStream entradaObjeto =  new ObjectInputStream(entrada);
		String recibido = "nada de nada";
		try {
			recibido = (String)entradaObjeto.readObject();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//entrada.read(buf);
		
		System.out.println("recib�: " + recibido);
	}

}