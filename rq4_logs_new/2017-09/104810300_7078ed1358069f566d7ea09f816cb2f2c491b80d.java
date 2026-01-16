package br.jus.tse.corporativa.io;

import java.io.File;

public class FileUtils {
	
	public boolean fileExists(String path) {
		File file = new File(path);
		return file.exists();
	}

}