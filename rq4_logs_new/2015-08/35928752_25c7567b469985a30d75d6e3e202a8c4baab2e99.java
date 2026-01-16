package ch.bfh.i4mi.handlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;
import org.springframework.ws.transport.http.HttpTransportConstants;
import org.springframework.xml.transform.TransformerObjectSupport;
import org.springframework.xml.xsd.XsdSchema;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * The Class XsdSchemaHttpHandler returns a XsdSchema on a GET-Request.
 * 
 * @author Kevin Tippenhauer, Berner Fachhochschule
 */
@SuppressWarnings("restriction")
public class XsdSchemaHttpHandler extends TransformerObjectSupport implements HttpHandler, InitializingBean {

    /** The Constant CONTENT_TYPE. */
    private static final String CONTENT_TYPE = "text/xml";

    /** The XSD Schema. */
    private XsdSchema schema;

    /** The xsdSchemaFolder. */
    private String xsdFolder;
    
    /**
     * Instantiates a new XsdSchemaHttpHandler.
     */
    public XsdSchemaHttpHandler() {
    }

    /**
     * Instantiates a new XsdSchemaHttpHandler.
     *
     * @param schema the schema
     */
    public XsdSchemaHttpHandler(XsdSchema schema) {
        this.schema = schema;
    }

    /**
     * Sets the schema.
     *
     * @param schema the new schema
     */
    public void setSchema(XsdSchema schema) {
        this.schema = schema;
    }
    
	/**
	 * Sets the location for the XSD Folder.
	 *
	 * @param serviceEndpoint the new location for the XSD Folder
	 */
	public void setXsdFolder(String xsdFolder) {
		this.xsdFolder = xsdFolder;
	}


	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
        Assert.notNull(schema, "'schema' is required");
    }

    /* (non-Javadoc)
     * @see com.sun.net.httpserver.HttpHandler#handle(com.sun.net.httpserver.HttpExchange)
     */
    public void handle(HttpExchange httpExchange) throws IOException {
        try {
            if (HttpTransportConstants.METHOD_GET.equals(httpExchange.getRequestMethod())) {
                Headers headers = httpExchange.getResponseHeaders();
                headers.set(HttpTransportConstants.HEADER_CONTENT_TYPE, CONTENT_TYPE);
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                transform(schema.getSource(), new StreamResult(os));
                String oss = new String(os.toByteArray());
                byte[] buf = oss.replace("[[XSD_FOLDER_PLACEHOLDER]]", xsdFolder).getBytes();
                httpExchange.sendResponseHeaders(HttpTransportConstants.STATUS_OK, buf.length);
                FileCopyUtils.copy(buf, httpExchange.getResponseBody());
            }
            else {
                httpExchange.sendResponseHeaders(HttpTransportConstants.STATUS_METHOD_NOT_ALLOWED, -1);
            }
        }
        catch (TransformerException ex) {
            logger.error(ex, ex);
        }
        finally {
            httpExchange.close();
        }
    }
}