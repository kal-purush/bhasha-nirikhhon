import java.io.*;
import java.net.*;
import java.applet.Applet;

public class TalkServer {

	public static int servlet(String path, PrintWriter os) {
		/*
	     * Java servlet
	     */
	    System.out.println("Client request path:" + path);

	    String html = "";

	    		/*
	    		 * JSP
	    		 */
			    if (path.equals("/")) {
					Reader reader = null;
		            char[] tempchars = new char[1024];
					try {
			            int charread = 0;
			            reader = new InputStreamReader(new FileInputStream("./index.html"));
			            while ((charread = reader.read(tempchars)) != -1) {
			                if ((charread == tempchars.length)
			                        && (tempchars[tempchars.length - 1] != '\r')) {
			                    System.out.print("tempchars:" + tempchars);
			                } else {
			                    for (int i = 0; i < charread; i++) {
			                        if (tempchars[i] == '\r') {*2
			                            continue;
			                        } else {
			                            // System.out.print("tempchars[i]:" + tempchars[i]);
			                            html += tempchars[i];
			                        }
			                    }
			                }
			            }

			        } catch (Exception e1) {
			            e1.printStackTrace();
			        } finally {
			            if (reader != null) {
			                try {
			                    reader.close();
			                } catch (IOException e1) {
			                }
			            }
			        }

			    	// html = tempchars.toString();
			    	// System.out.println(tempchars.toString());
			    } else if (path.equals("/shutdown")) {
			    	html = "<h1>Welcome to shutdown my computer!</h1>";
			    	try {
			    		Runtime.getRuntime().exec("shutdown -s -t 120");
			    	} catch(Exception e) {

			    	}
			    } else if (path.equals("/noshutdown")) {
			    	html = "<h1>Don\'t shutdown my computer!</h1>";
			    	try {
			    		Runtime.getRuntime().exec("shutdown -a");
			    	} catch(Exception e) {

			    	}
			    }

		String headers = "HTTP/1.1 200 OK\r\n" +
			"Date: Sun, 25 Jan 2015 12:46:58 GMT\r\n" +
			"Server: Timocat/2.2.22 (Win64) Java/8\r\n" +
			"X-Powered-By: Java/8\r\n" +
			"Content-Length: " + html.length() + "\r\n" +
			"Keep-Alive: timeout=5, max=99\r\n" +
			"Connection: Keep-Alive\r\n" +
			"Content-Type: text/html; charset=utf-8\r\n\r\n";

		String data = headers + html;

	    os.println(data);
	    os.flush();

	    return 0;
	}

	public static String getPath(String line) {
		String path = "";


		if (line == null) {
			System.out.println("[Null] Wrong line is null : ");
			return null;
		}

		if (line.length() < 1) {
			System.out.println("[Error] Wrong line length: " + line.length());
			return null;
		}

		String a = line.substring(4);

		if (a.length() < 1) {
			System.out.println("[Error] not corect substring line : " + line.length());
			return null;
		}

		int index = a.indexOf(" HTTP/");
		path = a.substring(0, index);

		if (path.length() < 1) {
			System.out.println("[Error] path error line: " + line);
			return null;
		}

		return path;
	}
 
	public static void main(String args[]) throws IOException {
    	int port = 8899;
    	ServerSocket server = new ServerSocket(port);
    	System.out.println("server listen...");

    	while(true) {

		    Socket socket = server.accept();
		    System.out.println("connection comming");
		    Reader reader = new InputStreamReader(socket.getInputStream());

		    System.out.println("start to read");

		    BufferedReader is =new BufferedReader(new InputStreamReader(socket.getInputStream()));
		    PrintWriter os =new PrintWriter(socket.getOutputStream());
		    BufferedReader sin =new BufferedReader(new InputStreamReader(System.in));

		    String line = is.readLine();
		    String path = TalkServer.getPath(line);

		    if (path == null) {
		    	continue;
		    }

		    TalkServer.servlet(path, os);

		    if (false) {
		    	break;
		    }

    		reader.close();
    		socket.close();
    		is.close();
    		sin.close();
		}

    	server.close();
    }
}