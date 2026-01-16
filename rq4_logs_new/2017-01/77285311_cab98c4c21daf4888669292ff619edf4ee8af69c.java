package multipart;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.log4j.Logger;

import data.ManifestFile;
import ui.Main;

/**
 * This class is for downloading the and merging the segments
 *
 */
public class Downloader {

	/** Logger used for testing and debugging purposes */
	final static Logger logger = Logger.getLogger(Downloader.class);

	public Downloader() {
	}

	/**
	 * Downloads a file from url
	 */
	public InputStream download(String urlStr) throws Exception {
		logger.info("In function Downloader.download()");
		InputStream inptStream = null;
		if (!Validator.validateURL(urlStr)) {
			throw new IOException("Invalid URL!");
		} else {
			logger.trace("The url " + urlStr + " is valid!");
		}
		urlStr = urlStr.trim();
		if (urlStr.endsWith(Main.MANIFEST_SUFFIX)) {
			logger.trace("The file in " + urlStr + " is of type manifest");
			List<String> fileSegemnts = readManifestFile(urlStr);
			for (int i = 0; i < fileSegemnts.size(); i++) {
				String[] linkList = fileSegemnts.get(i).split(";");
				inptStream = deepenDownload(inptStream, linkList);
			}
		} else {
			logger.trace("The file in " + urlStr + " is a normal file");
			getUrlContentAndMerge(inptStream, urlStr);
		}

		logger.info("Finished function download()");

		return inptStream;
	}

	/**
	 * Reads a manifest file and get its contents
	 */
	private List<String> readManifestFile(String url) throws MalformedURLException, IOException {
		// Get the manifest file that is addressed by url
		ManifestFile manifestFile = new ManifestFile(url);
		// Read File
		return manifestFile.readManifestFile();
	}

	/**
	 * Download the content of a url and go deeper to inner manifests if any
	 */
	public InputStream deepenDownload(InputStream inputStream, String[] urlContents) throws Exception// download
	{
		logger.info("In function manageDownload()");
		InputStream innerSegment = null;
		boolean success = false;
		// Get the content from only one machine, if found, don'e get from
		// alternatives
		for (String urlString : urlContents) {
			if (Validator.validateURL(urlString)) {
				try {
					if (urlString.endsWith(Main.MANIFEST_SUFFIX)) {
						innerSegment = download(urlString);
					} else {
						innerSegment = new URL(urlString).openStream();
					}
					if (inputStream == null) {
						inputStream = innerSegment;
					} else {
						inputStream = new SequenceInputStream(inputStream, innerSegment);

					}
					success = true;
					break; // Only from one machine
				} catch (Exception e) {
					// Continue to the next segment
				}
			}
		}
		if (!success) {
			logger.trace("Failed downloading a file content in " + urlContents.toString());
			throw new Exception("Failed!");
		}
		logger.info("Finished function manageDownload()");
		return inputStream;
	}

	/**
	 * Get the content of a url, and merge it with others if any
	 */
	private void getUrlContentAndMerge(InputStream inptStream, String urlString) throws Exception {
		logger.info("In function openUrlStreamAndMergeIt()");
		InputStream nextStream = new URL(urlString).openStream();
		if (inptStream == null) { // If there is only one segment
			inptStream = nextStream;
		} else { // sequence
			inptStream = new SequenceInputStream(inptStream, nextStream);
		}
		logger.info("Finished openUrlStreamAndMergeIt()");
	}

}