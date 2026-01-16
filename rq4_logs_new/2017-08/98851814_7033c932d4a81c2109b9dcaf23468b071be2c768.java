import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Util {
	public static boolean saveFileOverWrite(String filename, String contents) {
		try {
			File f = new File(filename);
			FileWriter writer = new FileWriter(f);

			writer.write(contents);
			;
			writer.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static boolean saveFileAppend(String filename, String contents) {
		try {
			File f = new File(filename);
			FileWriter writer = new FileWriter(f, true);

			writer.write(contents);
			writer.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static String readFile(String filename) {
		String data = "";

		try {
			File f = new File(filename);

			if (!f.exists()) {
				return "";
			}

			FileReader reader = new FileReader(f);

			while (reader.ready()) {
				data += (char) reader.read();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return data;
	}

}