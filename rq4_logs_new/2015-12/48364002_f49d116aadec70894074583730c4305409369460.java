package com.acme.imagedownloader;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;

/**
 * Main class for the application, requires 2 parameters, the URL of the website containing images and the location
 * of the folder for the images to be saved in. If the download folder does not exist it will be created.
 */
public class Downloader {

    public static void main(String[] args){
        if(args.length<2){
            System.out.println("Usage: Downloader [URL of website for images] [Output folder]");
        }else{
            String url = args[0];
            String outputFolder = args[1];
            createOutputFolderIfNecessary(outputFolder);
            callImageDownloadingService(url, outputFolder);
        }
    }

    private static void callImageDownloadingService(String url, String outputFolder) {
        ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
        ImageDownloadingService imageDownloadingService = ctx.getBean(ImageDownloadingService.class);
        int imagesDownloaded = imageDownloadingService.downloadImagesFromWebPage(url, outputFolder);
        System.out.println(imagesDownloaded + " images downloaded");
        ctx.close();
    }

    private static void createOutputFolderIfNecessary(String outputFolder) {
        File file = new File(outputFolder);
        if(!file.exists()){
            file.mkdirs();
        }
    }
}