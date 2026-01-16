/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.avalon.java.ocpjp.labs.actions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Scanner;

/**
 *
 * @author Constantine
 */
public class FileDeleteAction implements Action {
    
    private File file;
    
    public FileDeleteAction() {
        try {
        System.out.println("Input the source file's path to delete: ");
        Scanner sc = new Scanner(System.in);
        file = new File(sc.next());
        if (!file.exists()) {
            System.out.println("Source file does not exist");
        } 
        } catch (NullPointerException ex) {
            System.err.println(ex.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            synchronized (file) {
            Files.delete(file.toPath());
            System.out.println("File was deleted");
            }
        } catch (IOException | NullPointerException ex) {
            System.err.println(ex.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        //nothing to close
    }
    
}