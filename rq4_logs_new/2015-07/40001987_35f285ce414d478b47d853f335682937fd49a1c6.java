import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

public class Security {

	public static void main(String args[]) throws IOException {

		// 파일내용를 읽어들릴 객체 생성
		Scanner fileReader = new Scanner(new FileReader("info.txt"));

		// 파일내용를 가공하여 쓰기 위한 객체 생성
		PrintWriter fileWriter = new PrintWriter(new FileWriter(
				"C:/Users/user/Desktop/test.txt"));

		while (fileReader.hasNext()) {

			String line = fileReader.nextLine();

			String[] stArr = line.split("\n");

			for (int i = 0; i < stArr.length; i++) {
				if (stArr[i].contains("주민등록번호") || stArr[i].contains("신용카드")
						|| stArr[i].contains("운전면허번호")) {
					for (int j = 1; j < 10; j++) {
						String replace = stArr[i].replace("" + j, "*");
						stArr[i] = replace;
					}
				} else
					stArr[i] = stArr[i];

				fileWriter.print(stArr[i]);

				if (fileReader.hasNext())
					fileWriter.println();
			}
		}

		fileWriter.close();
		fileReader.close();

	}

}