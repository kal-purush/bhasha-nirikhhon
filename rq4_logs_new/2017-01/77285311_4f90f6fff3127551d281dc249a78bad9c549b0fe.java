import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

public class Downloader {
	
	/** Logger used for debugging and testing purposes */
	private static Logger log = Logger.getLogger(Downloader.class);


	/**
	 * The main method of downloading a manifest stream (which might have multiple parts)
	 * 
	 * @param urlStr
	 *            This is provided by the user
	 * @throws IOException 
	 */
	public void download(String urlStr) throws IOException {

		log.info("Starting to download the manifest file ...");
		URL url = new URL(urlStr);
		String streamPath = url.getPath();
		String fileType = streamPath.substring(streamPath.lastIndexOf('.') + 1);
		Multipart multipart = new Multipart();
		InputStream mainStream = multipart.openStream(url);

	}

	/**
	 * This method is to download a direct part (txt, jpg, ...)
	 * 
	 * @throws IOException
	 */
	public void downloadPart(InputStream mainStream) throws IOException {
		
		log.info("A file part is being downloaded ...");
		
		// An output stream where the data is written into a byte array
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		byte[] dataToWrite = new byte[512];
		int filePart = 0;
		while (filePart != -1) {
			outputStream.write(dataToWrite, 0, filePart); 
			filePart = mainStream.read(dataToWrite);
		}
		
		// TODO: Show the downloaded part
	}

}