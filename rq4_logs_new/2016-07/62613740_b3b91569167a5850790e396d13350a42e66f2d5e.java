package main;
import java.io.BufferedReader;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;


public class RemoteSSH {

	private Connection conn;
	private String ipAddr;
	private String charset = Charset.defaultCharset().toString();
	private String userName;
	private String password;
	
	public RemoteSSH(String ipAddr, String userName, String password, String charset){
		this.ipAddr = ipAddr;
		this.userName = userName;
		this.password = password;
		if(charset != null){
			this.charset = charset;
		}
	}
	
	/**
	 * 登陆远程主机
	 */
	public boolean login() throws IOException{
		conn = new Connection(ipAddr);
		conn.connect();
		return conn.authenticateWithPassword(userName, password);
	}
	private void  cleanExe(String cmd){
		Session session;
		try {
			session = conn.openSession();
			session.execCommand(cmd);
			InputStream in = new StreamGobbler(session.getStdout());
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = "";
			while((line=br.readLine())!=null){
				if(line.startsWith("clean"))
					break;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static String batch() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader("/home/"+RunOver.usr+"/configureset/slaves"));
		String line = "";
		String result = "";
		while((line=br.readLine())!=null){
			RemoteSSH rss = new RemoteSSH(line,RunOver.usr,RunOver.passwd,null);
			rss.login();
			rss.cleanExe("cd /home/"+RunOver.usr+"/cliquehadoop/ && ./putBkpb2dfs.sh bkpb");
			rss.closeConnection();
		}
		return result;
	}
	/**
	 * 执行shell脚本
	 */
	public String exec(String cmd){
		InputStream in = null;
		String result = "";
		try{
			//if(this.login()){
				Session session = conn.openSession();
				session.startShell();
				session.getStdin().write(cmd.getBytes());
				session.getStdin().write("\n".getBytes());
				session.getStdin().flush();
				//session.execCommand(cmd);
				in = new StreamGobbler(session.getStdout());
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String line = "";
				while((line=br.readLine())!=null){
					if(line.startsWith("Emit File size="))
						break;
				}
				result = line.split("=")[1];
				//br = new BufferedReader(new InputStreamReader(session.getStderr()));
				
				//System.out.println("stderr"+br.readLine());
				in.close();
			//}
		}catch(IOException e){
			
			e.printStackTrace();
		}
		return result;
	}
	private String parseStdout(InputStream in, String charset2) {
		byte[] buf = new byte[1024];
		StringBuffer sb = new StringBuffer();
		try{
			while(in.read(buf)!=-1){
				sb.append(new String(buf,charset));
			}
		}catch (IOException e){
			e.printStackTrace();
		}
		return sb.toString();
	}
	public void closeConnection(){
		conn.close();
	}
	
	public static double getRemoteFilesSize() throws NumberFormatException, IOException{
		double size = 0.0;
		BufferedReader br = new BufferedReader(new FileReader("/home/"+RunOver.usr+"/configureset/slaves"));
		String line = "";
		while((line=br.readLine())!=null){
			RemoteSSH rss = new RemoteSSH(line,RunOver.usr,RunOver.passwd,null);
			rss.login();
			String res = rss.exec("cd /home/"+RunOver.usr+"/cliquehadoop/ && java GetEmitFileSize bkpb");
			rss.closeConnection();
			size += Double.parseDouble(res);
		}
		return size;
	} 
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		System.out.println(RemoteSSH.getRemoteFilesSize());
		
	}

}