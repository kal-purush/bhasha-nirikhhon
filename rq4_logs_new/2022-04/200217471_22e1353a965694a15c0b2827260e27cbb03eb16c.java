package top.auzqy.design.pattern.creational.singleton.example01;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Logger {
  private FileWriter writer;
  private static final Logger instance = new Logger();

  private Logger() {
    File file = new File("/Users/wangzheng/log.txt");
    try {
		writer = new FileWriter(file, true);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} //true表示追加写入
  }
  
  public static Logger getInstance() {
    return instance;
  }
  
  public void log(String message) {
    try {
		writer.write(message);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
}

// Logger类的应用示例：