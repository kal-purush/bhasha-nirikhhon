package com.rumisystem.rumi_smtp.MODULE;

import static com.rumisystem.rumi_smtp.Main.CONFIG_DATA;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

public class ACCOUNT_Manager {
	private static HashMap<String, String> ACCOUNT_LIST = new HashMap<String, String>();

	public static void INIT() throws IOException {
		if (CONFIG_DATA.get("ACCOUNT").asString("MODE").equals("FILE")) {
			//ファイルモード
			List<String> ACCOUNT_TXT = Files.readAllLines(Paths.get(CONFIG_DATA.get("ACCOUNT").asString("PATH")));
			for (String LINE:ACCOUNT_TXT) {
				if (LINE.split(":").length == 2) {
					String ADDRESS = LINE.split(":")[0];
					String PASS = LINE.split(":")[1];
					ACCOUNT_LIST.put(ADDRESS, PASS);
				}
			}
		}
	}

	public static boolean Exists(String ADDRESS) {
		if (ACCOUNT_LIST.get(ADDRESS) != null) {
			return true;
		} else {
			return false;
		}
	}
}