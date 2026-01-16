
import java.io.*;
import java.util.*;

public class Lib {

	static int lines = 0;// 行数
	static int words = 0;// 单词数
	static int characters = 0;
	static ArrayList<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>();// 排序好的单词列表

	// 单词计算 计算行数、单词数和单词频率

	public static void wordCount(File f, int w) {

		BufferedReader br = null;
		HashMap<String, Integer> hashMap = new HashMap<String, Integer>();// 用于储存单词及其个数
		String str = null;
		String[] wordArray = null;
		int incre = (w == 1 ? 10 : 1);

		try {
			br = new BufferedReader(new FileReader(f));
			while ((str = br.readLine()) != null) {

				if (str.matches("[0-9]+")) {

					str = br.readLine();
					str = str.replaceFirst("Title: ", "");
					// 处理Title中的字符
					do {
						if (!str.trim().isEmpty()) {
							Lib.lines++;// 计算行数
						}
						Lib.characters++;// 把每行后的换行符加上
						Lib.characters += str.length();
						wordArray = str.split("\\s*[^a-zA-Z0-9]+");
						for (String word : wordArray) {
							if (word.matches("[a-zA-Z]{4,}[a-zA-Z0-9]*")) {
								Lib.words++;
								word = word.toLowerCase();

								if (hashMap.containsKey(word)) {
									hashMap.put(word, hashMap.get(word) + incre);// 计算单词出现次数
								} else {
									hashMap.put(word, incre);
								}
							}
						}
					} while ((str = br.readLine()) != null && !str.matches("Abstract: [\\s\\S]*"));
					if (str != null) {
						// 处理Abstract中的字符
						str = str.replaceFirst("Abstract: ", "");
						do {
							if (!str.trim().isEmpty()) {
								Lib.lines++;// 计算行数
							}
							Lib.characters++;// 把每行后的换行符加上
							Lib.characters += str.length();
							wordArray = str.split("\\s*[^a-zA-Z0-9]+");
							for (String word : wordArray) {
								if (word.matches("[a-zA-Z]{4,}[a-zA-Z0-9]*")) {
									Lib.words++;
									word = word.toLowerCase();
									if (hashMap.containsKey(word)) {
										hashMap.put(word, hashMap.get(word) + 1);// 计算单词出现次数
									} else {
										hashMap.put(word, 1);
									}
								}
							}
						} while ((str = br.readLine()) != null && !str.matches(""));
					} else {
						System.out.println("格式错误！");
						System.exit(0);
					}
					if (str != null) {
						if ((str = br.readLine()) != null && str.matches(""))
							continue;
						else {
							break;
						}

					} else {
						Lib.characters--;
						break;
					}
				}

			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		Lib.list = new ArrayList<Map.Entry<String, Integer>>(hashMap.entrySet());

		// 排序

		Collections.sort(Lib.list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
				int re = e2.getValue() - e1.getValue();
				return (re == 0) ? e1.getKey().compareTo(e2.getKey()) : re;

			}

		});

	}

	// 词组计算 计算行数、单词数和词组频率

	public static void phraseCount(File f, int w, int m) {

		BufferedReader br = null;
		HashMap<String, Integer> hashMap = new HashMap<String, Integer>();// 用于储存单词及其个数
		String str = null;
		String[] wordArray = null;// 单词数组
		String[] separator = null;// 分隔符数组
		StringBuffer sb = new StringBuffer();
		int count = 0;
		int incre = (w == 1 ? 10 : 1);

		try {
			br = new BufferedReader(new FileReader(f));
			while ((str = br.readLine()) != null) {

				count = 0;
				if (str.matches("[0-9]+")) {
					
					str = br.readLine();
					str = str.replaceFirst("Title: ", "");

					// 处理Title中的字符
					do {

						// 计算行数
						if (!str.trim().isEmpty()) {
							Lib.lines++;
						}

						// 计算字符数
						Lib.characters++;// 把每行后的换行符加上
						Lib.characters += str.length();

						// 计算词组词频
						str = str.toLowerCase();
						wordArray = str.split("\\s*[^a-zA-Z0-9]+");
						separator = str.split("[a-zA-Z0-9]+");
						for (int i = 0; i < wordArray.length; i++) {

							if (wordArray[i].matches("[a-zA-Z]{4,}[a-zA-Z0-9]*")) {
								Lib.words++;
								count++;
							} else {
								if (count >= m) {
									int tmpCount = count - m + 1;
									for (int j = 0; j < tmpCount; j++) {
										sb.delete(0, sb.length());
										System.out.println(i + " " + count + " " + j);
										sb.append(wordArray[i - count + j]);
										for (int k = 1, l = i - count + 1 + j; k < m; k++, l++) {
											sb.append(separator[l]).append(wordArray[l]);
										}
										String phrase = sb.toString();
										if (hashMap.containsKey(phrase)) {
											hashMap.put(phrase, hashMap.get(phrase) + incre);
										} else {
											hashMap.put(phrase, incre);
										}
									}
								}
								count = 0;
							}
						}

						if (count >= m) {
							int tmpCount = count - m + 1;
							int WALength = wordArray.length;
							for (int j = 0; j < tmpCount; j++) {
								sb.delete(0, sb.length());
								sb.append(wordArray[WALength - count + j]);
								for (int k = 1, l = WALength - count + 1 + j; k < m; k++, l++) {
									sb.append(separator[l]).append(wordArray[l]);
								}
								String phrase = sb.toString();
								if (hashMap.containsKey(phrase)) {
									hashMap.put(phrase, hashMap.get(phrase) + incre);
								} else {
									hashMap.put(phrase, incre);
								}
							}
						}

					} while ((str = br.readLine()) != null && !str.matches("Abstract: [\\s\\S]*"));
					if (str != null) {
						// 处理Abstract中的字符
						str = str.replaceFirst("Abstract: ", "");
						do {

							count = 0;
							// 计算行数
							if (!str.trim().isEmpty()) {
								Lib.lines++;
							}

							// 计算字符数
							Lib.characters++;// 把每行后的换行符加上
							Lib.characters += str.length();

							// 计算词组词频
							str = str.toLowerCase();
							wordArray = str.split("\\s*[^a-zA-Z0-9]+");
							separator = str.split("[a-zA-Z0-9]+");
							for (int i = 0; i < wordArray.length; i++) {

								if (wordArray[i].matches("[a-zA-Z]{4,}[a-zA-Z0-9]*")) {
									Lib.words++;
									count++;

								} else {
									if (count >= m) {
//											System.out.println(count);
										int tmpCount = count - m + 1;
										for (int j = 0; j < tmpCount; j++) {
											sb.delete(0, sb.length());
											sb.append(wordArray[i - count + j]);
											for (int k = 1, l = i - count + 1 + j; k < m; k++, l++) {
												sb.append(separator[l]).append(wordArray[l]);
											}
											String phrase = sb.toString();
											if (hashMap.containsKey(phrase)) {
												hashMap.put(phrase, hashMap.get(phrase) + 1);
											} else {
												hashMap.put(phrase, 1);
											}
										}
									}
									count = 0;
								}
							}
							if (count >= m) {
								int tmpCount = count - m + 1;
								int WALength = wordArray.length;
								for (int j = 0; j < tmpCount; j++) {
									sb.delete(0, sb.length());
									sb.append(wordArray[WALength - count + j]);
									for (int k = 1, l = WALength - count + 1 + j; k < m; k++, l++) {
										sb.append(separator[l]).append(wordArray[l]);
									}
									String phrase = sb.toString();
									if (hashMap.containsKey(phrase)) {
										hashMap.put(phrase, hashMap.get(phrase) + 1);
									} else {
										hashMap.put(phrase, 1);
									}
								}
							}
						} while ((str = br.readLine()) != null && !str.matches(""));
					} else {
						System.out.println("格式错误1！");
						System.exit(0);
					}
					if (str != null) {
						if ((str = br.readLine()) != null && str.matches(""))
							continue;
						else {
							break;

						}

					} else {
						Lib.characters--;
						break;

					}

				}

			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		Lib.list = new ArrayList<Map.Entry<String, Integer>>(hashMap.entrySet());

		// 排序

		Collections.sort(Lib.list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
				int re = e2.getValue() - e1.getValue();
				return (re == 0) ? e1.getKey().compareTo(e2.getKey()) : re;

			}

		});

	}


}