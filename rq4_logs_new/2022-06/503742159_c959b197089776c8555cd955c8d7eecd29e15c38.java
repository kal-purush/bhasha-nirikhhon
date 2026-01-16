package com.lockedme.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class FileLogics {
	String names[];
	int length;

	public void copyFile() {
		Scanner myObj = new Scanner(System.in);
		String fileName = null;
		System.out.println("ENTER SOURCE DIRECTORY");
		String inputdirectory = myObj.nextLine();
		System.out.println("ENTER INPUT FILE NAME");
		String inputfileName = myObj.nextLine();
		System.out.println("ENTER DESTINATION DIRECTORY");
		String outputdirectory = myObj.nextLine();
		System.out.println("ENTER OUTPUT FILE NAME");
		String outputfileName = myObj.nextLine();
		try {
			if(!inputdirectory.endsWith("/")) {
				inputdirectory=inputdirectory+"/";
			}
			if(!outputdirectory.endsWith("/")) {
				outputdirectory=outputdirectory+"/";

			}
			BufferedReader br = new BufferedReader(new FileReader(inputdirectory+inputfileName));
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputdirectory+outputfileName));
			int i;
			do {
				i = br.read();
				if (i != -1) {
					if (Character.isLowerCase((char) i))
						bw.write(Character.toUpperCase((char) i));
					else if (Character.isUpperCase((char) i))
						bw.write(Character.toLowerCase((char) i));
					else
						bw.write((char) i);
				}
			} while (i != -1);
			br.close();
			bw.close();
			System.out.println("FILE COPIED SUCCESSFULY");

		} catch (Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
	}

	public String deleteFile(String path) {
		try {
			File folder = new File(path);
			File[] listOfFiles = folder.listFiles();
			Scanner myObj = new Scanner(System.in);
			String fileName = null;
			for (int i = 0; i < listOfFiles.length; i++) {
				if (i == 0) {
					System.out.println("Enter the file name to be deleted");
					fileName = myObj.nextLine();
				}
				if (listOfFiles[i].isFile()) {
					if (fileName.equalsIgnoreCase(listOfFiles[i].getName())) {
						System.out.println("Type yes to confirm the action");
						String password = myObj.nextLine();
						if ("yes".equalsIgnoreCase(password)) {
							listOfFiles[i].delete();
							return "FILE DELETED.";
						} else {
							return "Delete operation cancelled.";
						}

					} else if (listOfFiles[i].isDirectory()) {
						System.out.println("Directory " + listOfFiles[i].getName());
					}
				}
			}
		} catch (Exception e) {
			return "Error at input path";
		}
		return "ERROR... FILE NOT FOUND, TRY AGAIN";
	}

	private List<File> sortFileFilter(String path) {
		try {
			List<File> files = Files.list(Paths.get(path)).map(Path::toFile).filter(File::isFile)
					.collect(Collectors.toList());
			return files;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public boolean sort(String path) {
		try {
			File directoryPath = new File(path);
			File files[] = directoryPath.listFiles();
			String filesname[] = directoryPath.list();
			String[] array = new String[files.length];
			for (int i = 0; i < array.length; i++) {
				array[i] = files[i].getName();
			}
			if (array == null || array.length == 0) {
				System.out.println("No files present at the mentioned directory");
				return false;
			}
			this.names = array;
			this.length = array.length;
			quickSort(0, length - 1);
			for (String file : filesname) {
				System.out.println(file);

			}
		} catch (Exception e) {
			System.out.println("This Directory is not present please check the path");
			return false;

		}
		return true;
	}

	void quickSort(int leastIndex, int higherIndex) {
		int lowindex = leastIndex;
		int highindex = higherIndex;
		String pivot = this.names[leastIndex + (higherIndex - leastIndex) / 2];

		while (lowindex <= highindex) {
			while (this.names[lowindex].compareToIgnoreCase(pivot) < 0) {
				lowindex++;
			}

			while (this.names[highindex].compareToIgnoreCase(pivot) > 0) {
				highindex--;
			}

			if (lowindex <= highindex) {
				exchangeNames(lowindex, highindex);
				lowindex++;
				highindex--;
			}
		}

		if (leastIndex < highindex) {
			quickSort(leastIndex, highindex);
		}
		if (lowindex < higherIndex) {
			quickSort(lowindex, higherIndex);
		}
	}

	void exchangeNames(int i, int j) {
		String temp = this.names[i];
		this.names[i] = this.names[j];
		this.names[j] = temp;
	}

}