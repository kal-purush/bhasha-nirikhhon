package org.milan.core.file;

import java.io.File;

/**
 * File Class Demo
 *
 * @author Milan Rathod
 */
public class FileDemo {

    public static void main(String[] a) {
        File f = new File("C:/FileDemo");

        if (!f.exists()) {
            try {
                f.createNewFile();
                System.out.println("Writable:-" + f.canWrite());
                System.out.println("Name:-" + f.getName());
                System.out.println("Readable:-" + f.canRead());
                System.out.println("Free Space:-" + f.getFreeSpace());
                System.out.println("Is it a Direstory:-" + f.isDirectory());
                System.out.println("Is it hidden ?:-" + f.isHidden());
                System.out.println("Length:-" + f.length());
            } catch (Exception e) {
                System.out.println(e.toString());
            }

        } else {
            System.out.println("Writable:-" + f.canWrite());
            System.out.println("Name:-" + f.getName());
            System.out.println("Readable:-" + f.canRead());
            System.out.println("Free Space:-" + f.getFreeSpace());
            System.out.println("Is it a Direstory:-" + f.isDirectory());
            System.out.println("Is it hidden ?:-" + f.isHidden());
            System.out.println("Length:-" + f.length());

        }
    }

}