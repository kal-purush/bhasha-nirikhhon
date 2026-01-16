/******************************************************************
 * File:        NotifyAction.java
 * Created by:  Dave Reynolds
 * Created on:  2 Mar 2015
 * 
 * (c) Copyright 2015, Epimorphics Limited
 *
 *****************************************************************/

package com.epimorphics.dmsworker.actions;

import org.apache.jena.atlas.json.JsonObject;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.epimorphics.appbase.tasks.impl.BaseAction;
import com.epimorphics.dmsworker.ActionProcessor;
import com.epimorphics.json.JsonUtil;
import com.epimorphics.tasks.ProgressMonitorReporter;

/**
 * Send SMS notification to "topicARN"
 * The "target" will be the message subject, and the request body will be the message mody. 
 */
public class NotifyAction extends BaseAction {
    public static final String TOPIC_ARN_PARAM = "topicARN";
    
    @Override
    protected JsonObject doRun(JsonObject parameters,
            ProgressMonitorReporter monitor) {
        String topicARN = getStringParameter(parameters, TOPIC_ARN_PARAM);
        String subject = getStringParameter(parameters, ActionProcessor.TARGET_PARAM);
        String message = getStringParameter(parameters, ActionProcessor.MESSAGE_PARAM);
        AmazonSNSClient client = new AmazonSNSClient();
        client.setRegion( Regions.EU_WEST_1 );
        client.publish(topicARN, message, subject);
        return JsonUtil.emptyObject();
    }
    

}