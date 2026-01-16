package com.wayleynam.httpd.core;

import com.wayleynam.httpd.response.Status;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Created by wei4liverpool on 9/29/16.
 * basic class for a http server
 */
public abstract class Httpd {

    //校验header中的Content-Disposition,该header返回response消息为attachement等
    public static final String CONTENT_DISPOSITION_REGEX = "([ |\t]*Content-Disposition[ |\t]*:)(.*)";
    public static final Pattern CONTENT_DISPOSITION_PATTERN = Pattern.compile(CONTENT_DISPOSITION_REGEX, Pattern.CASE_INSENSITIVE);


    public static final String CONTENT_TYPE_REGEX = "([ \t]*content-type[ |\t]*:)(.*)";
    public static final Pattern CONTENT_TYPE_PATTERN = Pattern.compile(CONTENT_TYPE_REGEX, Pattern.CASE_INSENSITIVE);

    public static final String CONTENT_DISPOSITION_ATTRIBUTE_REGEX = "[ |\t]*([a-zA-Z]*)[ |\t]*=[ |\t]*['|\"]([^\"^']*)['|\"]";

    public static final Pattern CONTENT_DISPOSITION_ATTRIBUTE_PATTERN = Pattern.compile(CONTENT_DISPOSITION_ATTRIBUTE_REGEX);


    public static final class ResponseException extends Exception {

        private static final long serialVersionUID = 1L;

        private final Status status;


        public ResponseException(Status status, String message) {
            super(message);
            this.status = status;
        }


        public ResponseException(Status status, String message, Exception e) {
            super(message, e);
            this.status = status;
        }

        public Status getStatus() {
            return this.status;
        }

    }


    public final static int SOCKET_READ_TIMEOUT = 5000;

    public final static String MIME_PLAINTEXT = "text/plain";

    public final static String MIME_HTML = "text/html";

    public final static String QUERY_STRING_PARAMETER = "NanoHttpd.QUERY_STRING";

    public static Logger logger = Logger.getLogger(Httpd.class.getName());

    protected Map<String, String> MIME_TYPES;


    /**
     * 获取资源文件
     * @param mime_types
     * @param resourceName
     */
    private static void loadMimeTypes(Map<String,String> mime_types,String resourceName){
        try {
            Enumeration<URL> resourceEnum=Httpd.class.getClassLoader().getResources(resourceName);
            while(resourceEnum.hasMoreElements()){
                URL url=(URL)resourceEnum.nextElement();
                Properties properties=new Properties();
                InputStream is=null;

                try{
                    is=url.openStream();
                    properties.load(is);

                }catch (IOException e){
                    logger.log(Level.INFO,"cannot load mime_type resources");
                }finally {
                    safeClose(is);
                }
                mime_types.putAll((Map)properties);

            }
        } catch (IOException e) {
            logger.log(Level.INFO,"cannot load mime_type resources");
        }


    }



    public final static void safeClose(Object closable){
        try{
            if(closable!=null){
                if(closable instanceof Closeable){
                    ((Closeable) closable).close();
                }else if (closable instanceof Socket){
                    ((Socket)closable).close();
                }else if(closable instanceof ServerSocket){
                    ((ServerSocket) closable).close();
                }

            }


        }catch (Exception e){
            logger.log(Level.SEVERE,"cannot close stream,e:"+e.getMessage());
        }

    }


}

