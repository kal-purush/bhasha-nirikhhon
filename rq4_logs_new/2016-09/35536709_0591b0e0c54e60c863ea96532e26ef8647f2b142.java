package os;

import os.init.InitProcess;
import os.init.RootFolder;

/**
 *
 */
public class Main {

	public static void main(String[] args) {
		RootFolder rootFolder = new RootFolder();
		new InitProcess().start(rootFolder, rootFolder, new String[0]);
	}

}