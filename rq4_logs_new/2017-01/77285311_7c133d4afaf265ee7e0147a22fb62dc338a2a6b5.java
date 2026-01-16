package data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;

import org.apache.log4j.Logger;

public class ManifestFile {

	/** A logger used for testing and debugging purposes */
	final Logger logger = Logger.getLogger(ManifestFile.class);

	/** URL of the manifest file */
	private String manifestPath = "";
	/** URL of inner segments */
	private LinkedList<String> innerSegmentsUrlList = new LinkedList<String>();

	/** The separator (**) between the segments */
	private final static String separator = "**";

	public ManifestFile(String path) {
		this.manifestPath = path;
	}

	/**
	 * Read the manifest file which has inner segments or other manifest files
	 * separated by two starts (**)
	 * 
	 * @return
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public LinkedList<Segment> readManifestFile() throws IOException, MalformedURLException {
		logger.trace("Reading the manifest file with path: " + manifestPath);

		URL url = new URL(manifestPath);
		BufferedReader manifestFileData = new BufferedReader(new InputStreamReader(url.openStream()));
		String line = "";
		while ((line = manifestFileData.readLine()) != null) {
			// Exclude the separator (**)
			if (!line.equals(separator)) {
				innerSegmentsUrlList.add(line);
			}
		}
		logger.trace("Finished reading the manifest file with path: " + manifestPath);

		LinkedList<String> innerUrlsList = refineSegmentsUrls(innerSegmentsUrlList);
		LinkedList<Segment> segments = readSegmentsUrls(innerUrlsList);
		manifestFileData.close();
		return segments;
	}

	/**
	 * 
	 * @param innerUrlsList
	 * @return
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public LinkedList<Segment> readSegmentsUrls(LinkedList<String> innerUrlsList)
			throws IOException, MalformedURLException {
		logger.trace("Reading the segments of the manifest file with path: " + manifestPath);

		LinkedList<Segment> segmentsData = new LinkedList<Segment>();

		for (String innerUrl : innerUrlsList) {
			segmentsData.add(new Segment(innerUrl));
		}

		logger.trace("Finished reading the segments of the manifest file with path: " + manifestPath);

		return segmentsData;
	}

	/**
	 * Remove useless extensions from segments urls
	 * 
	 * @param segmentsUrlList
	 * @return
	 */
	public LinkedList<String> refineSegmentsUrls(LinkedList<String> segmentsUrlList) {
		logger.trace("Refining the urls of the segments urls of the manifest file with path: " + manifestPath);

		LinkedList<String> refinedSegmentsUrls = new LinkedList<String>();
		for (int i = 0; i < segmentsUrlList.size(); i++) {
			// Get valid URL without other extensions, like -segments
			String refinedUrl = segmentsUrlList.get(i).substring(0, segmentsUrlList.get(i).lastIndexOf('.'));
			refinedSegmentsUrls.add(refinedUrl);
		}
		logger.trace("Finished refining the segments urls of the manifest file with path: " + manifestPath);

		return refinedSegmentsUrls;
	}
}