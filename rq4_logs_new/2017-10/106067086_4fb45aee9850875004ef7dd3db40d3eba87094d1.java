import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class PhysLayerClient {
	private static final String[] 4B5BTable = new 4B5BTable[15];

	public static void main(String[] args) {
		try (Socket socket = new Socket("18.221.102.182", 38002)) {
			InputStream is = socket.getInputStream();
			OutputStream os = socket.getOutputStream();
			System.out.println("Connected to server.");
			setTable();

			int[] receivedBytes
			double baseline = getBaseline(is);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void setTable() {
		4B5BTable[0] = "11110";
		4B5BTable[1] = "01001";
		4B5BTable[2] = "10100";
		4B5BTable[3] = "10101";
		4B5BTable[4] = "01010";
		4B5BTable[5] = "01011";
		4B5BTable[6] = "01110";
		4B5BTable[7] = "01111";
		4B5BTable[8] = "10010";
		4B5BTable[9] = "10011";
		4B5BTable[10] = "10110";
		4B5BTable[11] = "10111";
		4B5BTable[12] = "11010";
		4B5BTable[13] = "11011";
		4B5BTable[14] = "11100";
		4B5BTable[15] = "11101";
	}

	private static double getBaseline(InputStream inputStream) {
		double avg = 0;
		double val;
		for (int = 0; i < 64; i++) {
			val = (double) inputStream.read();
			avg += val;
		}

		avg /= 64;
		return avg;
	}

}