package main;

import keygen.Key;
//TODO import org.apache.commons.net.ftp.*;
import javax.swing.JOptionPane;
import java.io.File;

public class main {
	public static String GenerateKey(int strength){
		Key key = new Key(strength);
		key.pGen();
		key.qGen();
		key.nGen();
		key.dGen();
		key.PublicKeyGen();
		key.PrivateKeyGen();
		key.SaveKey();
		return String.valueOf(key.key_length);
	}
	public static void Encryption(String key_file, String filepath){
		Encrypt x = new Encrypt(key_file, filepath);
		x.ConvertFile();
		x.EncryptFile();
		x.SaveFile();
		x.DeleteFile();
	}
	public static void Decryption(String filepath){
		Decrypt x = new Decrypt(filepath);
		x.DecryptFile();
		x.CreateFile();
		x.DeleteFile();
	}
	public static String[] ShowFiles(String folderpath){
		File folder = new File(folderpath);
		File[] listOfFiles = folder.listFiles();
		String[] files = new String[listOfFiles.length];
		for (int i = 0; i < listOfFiles.length; i++){
			if (listOfFiles[i].isFile()){
			   files[i] = listOfFiles[i].getName();
			}	
		}
		return files;
	}
	public static void main( String[] args){
		String[] chooseprogram = {"Encryption", "Decryption", "Create key"};
		Object selectedValue = JOptionPane.showInputDialog(null,
		        "Choose option:", "RSA",
		        JOptionPane.PLAIN_MESSAGE, null,
		        chooseprogram, chooseprogram[0]);
		if (selectedValue == "Create key"){
			int strength = Integer.parseInt(JOptionPane.showInputDialog("Enter desired strength:"));
			String key_length = GenerateKey(strength);
			JOptionPane.showMessageDialog(null,"Key generated.\nKey Length: " + key_length);
		}
		else if (selectedValue == "Encryption"){
			String filepath = JOptionPane.showInputDialog("Please enter filepath for file you wish to encrypt:");
			String[] folder_contents = ShowFiles("/home/david/keys/");
			Object key_chosen = JOptionPane.showInputDialog(null,
			        "Choose key to encrypt with:", "RSA",
			        JOptionPane.PLAIN_MESSAGE, null,
			        folder_contents, folder_contents[0]);
		    Object[] options = { "OK", "Cancel" };
		    int cont = JOptionPane.showOptionDialog(null, "Are you sure you wish to encrypt this file?", "Warning",
		            JOptionPane.DEFAULT_OPTION, JOptionPane.WARNING_MESSAGE,
		            null, options, options[0]);
		    if (cont == 0){
		    	Encryption("/home/david/keys/" + key_chosen.toString(), filepath);
		    	JOptionPane.showMessageDialog(null,"Encryption Complete");
		    }
		}
		else if (selectedValue == "Decryption"){
			String filepath_decryption = JOptionPane.showInputDialog("Please enter filepath for file you wish to decrypt:");
			Decryption(filepath_decryption);
			JOptionPane.showMessageDialog(null,"Decryption Complete");
		}
		System.exit(0);
	}			
}
 