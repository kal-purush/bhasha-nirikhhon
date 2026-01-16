package com.tirthhihoriya;

import java.io.*;
import java.util.Scanner;

public class Login {

    String uid;
    String pass;

    public void register(Login u) throws IOException
    {
        Scanner sca = new Scanner(System.in);
        System.out.println("\n\n\n---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");

        System.out.println("\n\t\t\t\t\t\t\t\t\t\t\tWelcome! New User.\n\n");
        System.out.print("\t\t\t\t\t\t\t\t\t\t\tSet User Id : ");
        u.uid = sca.nextLine();
        System.out.print("\t\t\t\t\t\t\t\t\t\t\tSet pass: ");
        u.pass=sca.nextLine();

        File f2= new File("./"+u.uid);

        if(f2.mkdirs())	//if subdir is not created handle it through exception;
        {
            System.out.println("\n\n\t\t\t\t\t\t\t\t\t\tYou are successfully registered!!\n\t\t\t\t\t\t\t\t\tNote:- Please remember your details for future use.");
            System.out.println("---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        }
        File f3= new File("./"+u.uid+"/pass.txt");
        f3.createNewFile();

        BufferedWriter bf =new BufferedWriter(new FileWriter(f3));
        bf.write(u.pass);
        //f2.close();
        bf.close();
    }

    public boolean login(Login u) throws IOException
    {
        Scanner sca = new Scanner(System.in);
        int x;
        System.out.println("\n\n\n---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");

        System.out.println("\t\t\t\t\t\t\t\t\t\t\tEnter your login details.\n");
        System.out.print("\t\t\t\t\t\t\t\t\t\t\tUser Id: ");
        u.uid=sca.nextLine();
        System.out.print("\t\t\t\t\t\t\t\t\t\t\tPassword: ");
        u.pass=sca.nextLine();

        File f5 =new File("./"+u.uid);
        BufferedReader br = new BufferedReader(new FileReader("./"+u.uid+"/pass.txt"));
        String read = br.readLine();
        //System.out.println(read);
        br.close();
        System.out.println("---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");

        if(f5.isDirectory()&&read.equals(u.pass))
            return true;
        else
            return false;
    }

}