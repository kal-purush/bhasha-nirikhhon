package file;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URLEncoder;

import com.opensymphony.xwork2.ActionSupport;

public class fileDownloadAction extends ActionSupport {
	private InputStream inputStream;
	private String contentDisposition;
	private long contentLength;
	
	private String fileDownloadPath = "C:\\upload\\";
	private String fileDownloadName = "testCode.txt";
	
	public String execute() throws Exception {
		File fileInfo = new File(fileDownloadPath + fileDownloadName);
		
		setContentLength(fileInfo.length());
		setContentDisposition("attachment; filename="+URLEncoder.encode(fileDownloadName,"UTF-8"));
		setInputStream(new FileInputStream(fileDownloadPath + fileDownloadName));
		
		return SUCCESS;
	}

	public InputStream getInputStream() {
		return inputStream;
	}

	public void setInputStream(InputStream inputStream) {
		this.inputStream = inputStream;
	}

	public String getContentDisposition() {
		return contentDisposition;
	}

	public void setContentDisposition(String contentDisposition) {
		this.contentDisposition = contentDisposition;
	}

	public long getContentLength() {
		return contentLength;
	}

	public void setContentLength(long contentLength) {
		this.contentLength = contentLength;
	}

	public String getFileDownloadPath() {
		return fileDownloadPath;
	}

	public void setFileDownloadPath(String fileDownloadPath) {
		this.fileDownloadPath = fileDownloadPath;
	}

	public String getFileDownloadName() {
		return fileDownloadName;
	}

	public void setFileDownloadName(String fileDownloadName) {
		this.fileDownloadName = fileDownloadName;
	}
	
	
}