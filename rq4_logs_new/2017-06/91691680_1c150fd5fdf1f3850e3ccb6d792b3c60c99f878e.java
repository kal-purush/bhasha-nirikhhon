package scan;

import java.io.File;
import java.util.ArrayList;

public class ScanManager {

	private int windowSize;
	private int batch;

	private Scanner scanner;
	private ArrayList<String> fList;

	public ScanManager(int windowSize, File file, int xi) {
		this.windowSize = windowSize;

		batch = 0;
		scanner = new Scanner(xi, file);
		fList = scanner.scanFirstWindow(windowSize);

	}

	public ArrayList<ArrayList<String>> scanNextWindow() {
		ArrayList<ArrayList<String>> list = scanner.scanLines(windowSize, batch, fList);
		batch++;
		return list;

	}

	public static void main(String[] args) {
		Scanner s = new Scanner(0, new File("market_data.csv"));

		int windowSize = 10;

		ArrayList<String> fList = s.scanFirstWindow(windowSize);
		System.out.println(fList);
		System.out.println(s.scanLines(windowSize, 1, fList));

	}

}