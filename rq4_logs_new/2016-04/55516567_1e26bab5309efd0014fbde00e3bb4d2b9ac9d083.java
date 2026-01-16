/**
 * Copyright (C) 2016 MIT Libraries
 * Licensed under: http://www.apache.org/licenses/LICENSE-2.0
 */
package edu.mit.lib.felt;

import org.apache.camel.Header;

/**
  * Bean for assigning resource names from Message data
  * @author richardrodgers
  */

public class ResourceName {

    public String baseName(@Header("CamelFileName") String fileName) {
        return name(fileName, "base");
    }

    public String filesName(@Header("CamelFileName") String fileName) {
        return name(fileName, "files");
    }

    public String fileName(@Header("CamelFileName") String fileName) {
        return name(fileName, "file");
    }

    private String name(String fileName, String rtype) {
        String[] parts = fileName.split("/");
        String stem = parts[1].substring(0, parts[1].lastIndexOf("."));
        StringBuilder sb = new StringBuilder("/");
        sb.append(parts[0]).append("/pages/").append(stem);
        switch(rtype) {
            case "base": break;
            case "files": sb.append("/files"); break;
            case "file": sb.append("/files/").append(parts[1]); break;
            default: break;
        }
        return sb.toString();
    }
}