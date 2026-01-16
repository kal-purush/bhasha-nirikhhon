package api;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

/**
 * Informação sobre servidor.
 */
public class Endpoint {
	/**
	 * URL do servidor
	 */
	private String url;
	/**
	 * Dicionario com informação sobre o servidor.
	 */
	private Map<String, Object> attributes;

	public Endpoint() {
	}

	public Endpoint(String url, Map<String, Object> attributes) {
		this.url = url;
		this.attributes = attributes;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void setAttributes(Map<String, Object> attributes) {
		this.attributes = attributes;
	}

	public String getUrl() {
		return url;
	}

	public Map<String, Object> getAttributes() {
		return attributes;
	}

	public String toString() {
		return String.format("%s : %s", url, attributes);
	}

	/**
	 * Devolve identificador gerado deterministicamente a partir do URL.
	 */
	public String generateId() {
		try {
			byte[] hash = MessageDigest.getInstance("MD5").digest(url.getBytes());
			return DatatypeConverter.printHexBinary(hash);
		} catch (NoSuchAlgorithmException e) {
			return "" + (url.hashCode() >>> 1);
		}

	}
}