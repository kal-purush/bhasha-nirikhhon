
/**
 * DataServiceFault.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis2 version: 1.6.1-wso2v10  Built on : Sep 04, 2013 (02:02:54 UTC)
 */

package com.swifta.sub.mats.reporting;

public class DataServiceFault extends java.lang.Exception{

    private static final long serialVersionUID = 1429568857092L;
    
    private com.swifta.sub.mats.reporting.MatsreportingserviceStub.DataServiceFault faultMessage;

    
        public DataServiceFault() {
            super("DataServiceFault");
        }

        public DataServiceFault(java.lang.String s) {
           super(s);
        }

        public DataServiceFault(java.lang.String s, java.lang.Throwable ex) {
          super(s, ex);
        }

        public DataServiceFault(java.lang.Throwable cause) {
            super(cause);
        }
    

    public void setFaultMessage(com.swifta.sub.mats.reporting.MatsreportingserviceStub.DataServiceFault msg){
       faultMessage = msg;
    }
    
    public com.swifta.sub.mats.reporting.MatsreportingserviceStub.DataServiceFault getFaultMessage(){
       return faultMessage;
    }
}
    