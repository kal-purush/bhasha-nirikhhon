import java.applet.*;
import java.net.*;
import java.io.*;

/*
 * Reads an entity using an HTTP partial GET command
 * I had to develop this because the Microsoft VM cannot
 * perform a partial get from an applet 
 */
public class HttpGetReader {
	
	// reference to the ptviewer object
	private Applet ptv;
	
	// host and port to connect to
	private String host;
	private int port;

	public HttpGetReader( Applet ptv ) {
		this.ptv = ptv;
		port = ptv.getCodeBase().getPort();
        if (port < 0) port = 80;
        host = ptv.getCodeBase().getHost();

	}

	// extract the status code from the first response line
	private int getStatusCode( String line ) {
		String code = line.substring( 8 ).trim();
		code = code.substring( 0, 3 );
		return Integer.valueOf(code).intValue();
	}
	
	// gets the specified range of bytes from the file
	// returns null if something goes wrong
	public byte[] doPartialGet( String fileName, int start, int nBytes ) {
		String line, header, range;
		
        try {
			Socket sock = new Socket(host, port);
//	        System.out.println( "Socket: " + host + " " + port );
	        DataInputStream inStream = new DataInputStream( sock.getInputStream() );
	        DataOutputStream outStream = new DataOutputStream( sock.getOutputStream() );
	        URL u = new URL( ptv.getDocumentBase(), fileName );
	        header = "GET " + u.getFile() + " HTTP/1.1\r\n";
	        header += "User-Agent: PTViewer\r\n";
			range = "Range: bytes=" + start + "-" + (start + nBytes - 1);
			header += range + "\r\n";
			header += "Connection: close\r\n";
			header += "Host: " + host + ":" + port + "\r\n\r\n"; 
//	        System.out.println( header );
	        outStream.writeBytes( header );

	        // reads the response header
	        int len = -1;
            int responseCode = 100;	// to run the loop for the first time
            while( responseCode == 100 ) {
            	responseCode = -1;
	            while( (line = inStream.readLine()) != null ) {
	            	if( line.length() == 0 ) 
	            		break;	// end of header
//	            	System.out.println( line );
	            	if( responseCode == -1 ) responseCode = getStatusCode( line );
	            	if( line.toLowerCase().startsWith("content-length:") ) {
	            		try {
	            			len = Integer.valueOf(line.substring(15).trim()).intValue();
	            		} catch (Exception ex) {}
	            	}
	            }
            }
//            System.out.println( "Status=" + responseCode + "    len=" + len );
            
            // checks the response values
            if( responseCode != 206 ) {
            	System.out.println( "PTViewer: unexpected response code: " + responseCode );
            	return null;
            }
            if( len != nBytes ) {
            	System.out.println( "PTViewer: number of returned bytes does not match. Requested: " + nBytes + "   Returned in header: " + len );
            	return null;
            }
            
            // reads the response body
            ByteArrayOutputStream ba;
    		if( len < 0 )
    			ba = new ByteArrayOutputStream();
    		else
    			ba = new ByteArrayOutputStream( len );
            		
    		int tmpLen;
    		byte[] tmpBuf = new byte[4096];
    		while ((tmpLen = inStream.read(tmpBuf)) >= 0) {
    			ba.write( tmpBuf, 0, tmpLen );
            }
    		ba.close();
    		byte[] retVal = ba.toByteArray();
    		len = retVal.length;
    		if( len != nBytes ) {
            	System.out.println( "PTViewer: number of returned bytes does not match. Requested: " + nBytes + "   Returned in body: " + len );
            	return null;
            }
    		return retVal;
		}
		catch (Exception ex ) {
			ex.printStackTrace();
			return null;
		}
	}
	
}