/*
 Copyright 2016  Simon Farrow <simon.farrow1@hscic.gov.uk>

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package uk.nhs.digital.mait.tkwx.httpinterceptor.spinemth;

import uk.nhs.digital.mait.tkwx.tk.handlers.*;
import java.io.StringReader;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import net.sf.saxon.dom.NodeOverNodeInfo;
import net.sf.saxon.om.NodeInfo;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import uk.nhs.digital.mait.tkwx.tk.boot.HttpTransport;
import org.xml.sax.InputSource;

/**
 *
 * @author simonfarrow
 */
public abstract class AbstractSoapRequestHandler extends uk.nhs.digital.mait.tkwx.tk.boot.ToolkitHttpHandler {
    protected String synchronousWrapper = null;
    protected String soapfault = null;
    
    protected XPathExpression getMessageID = null;
    protected XPathExpression getReplyTo = null;
    protected XPathExpression getFaultTo = null;
    protected XPathExpression getTo = null;
    protected XPathExpression getHeader = null;
    protected XPathExpression getBody = null;
    protected XPathExpression getSoapRequest = null;
    
    protected static final String SOAPREQUEST = "/";

    
    protected int getAsyncTTL() {
        return super.asyncttl;
    }

    protected synchronized Node extractHeader(Document d)
            throws Exception {
        return (Node) getHeader.evaluate(d, XPathConstants.NODE);
    }

    protected synchronized Node extractBody(Document d)
            throws Exception {
        return (Node) getBody.evaluate(d, XPathConstants.NODE);
    }

    protected Node extractSoapRequest(Document d)
            throws Exception {
        return (Node) getSoapRequest.evaluate(d, XPathConstants.NODE);
    }

    protected String extractMessageId(String m)
            throws Exception {
        InputSource s = new InputSource(new StringReader(m));
        return extractStringXpath(getMessageID, s);
    }

    protected String extractReplyTo(String m)
            throws Exception {
        InputSource s = new InputSource(new StringReader(m));
        return extractStringXpath(getReplyTo, s);
    }

    protected String extractFaultTo(String m)
            throws Exception {
        InputSource s = new InputSource(new StringReader(m));
        String ft = extractStringXpath(getFaultTo, s);
        return (ft.length() == 0) ? null : ft;
    }
    
    protected synchronized Node extractNodeXpath(XPathExpression x, InputSource m)
            throws Exception {
        // this fails with cannot cast saxon tinyElementImpl to dom Node
        // see http://stackoverflow.com/questions/1985509/saxon-xpath-api-returns-tinyelementimpl-instead-of-org-w3c-dom-node
        //return (Node) x.evaluate(m, XPathConstants.NODE);
        
        // this is recommended workaround
        return (Node)NodeOverNodeInfo.wrap((NodeInfo) x.evaluate(m, XPathConstants.NODE));
    }

    protected synchronized String extractStringXpath(XPathExpression x, InputSource m)
            throws Exception {
        return (String) x.evaluate(m, XPathConstants.STRING);
    }

    protected String getSoapFault() {
        return soapfault;
    }

    protected String getSynchronousWrapper() {
        return synchronousWrapper;
    }

    protected String getSavedMessagesDirectory() {
        return savedMessagesDirectory;
    }


    protected HttpTransport getToolkit() {
        return super.toolkit;
    }

}