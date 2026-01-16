package testWatson;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class testProcess {

	public static void main(String[] args) throws IOException, InterruptedException {
		Class klass = datagramAudio.Client.class;
		String javaHome = System.getProperty("java.home");
		String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
		String classpath = System.getProperty("java.class.path");
		String className = klass.getCanonicalName();

		ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", classpath, className, "localhost");

		Process process = builder.start();
		BufferedReader in = new BufferedReader(new InputStreamReader(process.getErrorStream()));
		String line;
		while ((line = in.readLine()) != null) {
			System.out.println(line);
		}
		process.waitFor();
		System.out.println(process.exitValue());

	}

}