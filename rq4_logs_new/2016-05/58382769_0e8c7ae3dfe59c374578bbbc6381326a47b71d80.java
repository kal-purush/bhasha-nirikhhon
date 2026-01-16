package com.vccorp.bigdata.adsmobile;

/**
 * Created by quanpv on 5/29/16.
 */

import com.scientiamobile.wurfl.Device;
import com.scientiamobile.wurfl.Wurfl;
import net.sourceforge.wurfl.core.GeneralWURFLEngine;
import net.sourceforge.wurfl.core.cache.LRUMapCacheProvider;
import scala.collection.immutable.Map;



public class UserAgentParser {


    public static Wurfl wurflWrapper = new Wurfl(new GeneralWURFLEngine("file:///home/quanpv/Downloads/wurfl.zip"));

    public static void main(String[] args){

        String userAgentString = "Mozilla/5.0 (iPad; CPU OS 9_3_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13E238 Safari/601.1";

        String s = UserAgentParser.getUserAgentString(userAgentString);
        System.out.println(s);
    }

    public static String getUserAgentString(String userAgent) {
//
        wurflWrapper.setCacheProvider(new LRUMapCacheProvider());

        wurflWrapper.setTargetAccuracy();
        Device device = wurflWrapper.deviceForRequest(userAgent);
        Map map = device.capabilities();

        String device_os = (String)map.getOrElse("device_os", null);
        String device_os_version;

        try {
            device_os_version = UserAgentHelper.getOsVersion(userAgent);
            if(device_os_version.trim()==""){
                device_os_version = (String)map.getOrElse("device_os_version", null);
            }
        } catch (Exception e){
            device_os_version = "-1";
        }
        String model_name = (String)map.getOrElse("model_name", null);
        String brand_name = (String)map.getOrElse("brand_name", null);
        String parse = device.id() + "\t" + device_os + "\t" + device_os_version + "\t" + model_name + "\t" + brand_name;

        return parse;
    }

}