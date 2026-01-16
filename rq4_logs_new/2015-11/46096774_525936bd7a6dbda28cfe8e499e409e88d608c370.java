package com.rj.file;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import com.rj.tools.UnixTime;

public class FileTools {

	public FileTools() {
	}
	
	/**************************************************
	***************************************************/
	
	/**
	 * �ڸ�Ŀ¼���½�temp�ļ���
	 * @param location ����ļ�����λ���Ƿ���ڣ�û���򴴽�
	 */
	public void builtFile(String location){
		String Save_Location;
		Save_Location= location;
		if (!(new File( Save_Location).isDirectory() ) ){ //����ļ��в����ڣ����½�
			 File myFilePath = null;
			 myFilePath = new File(Save_Location);
			 myFilePath.mkdir(); 
		}
	}
	
	/**
	 * �����ı�
	 * @param builtNum  �½��ı��ĸ���
	 * @param location  �ı�����λ��
	 */
	public void builtTxt(int builtNum, String location ){
		PrintWriter out = null;
		String unixtime = null;
		for (int i = 0; i < builtNum; i++) {
			try {
				unixtime = UnixTime.toUnixTime();
			
				String filename = location+"//�ļ�"+unixtime+".txt";
				if (!(new File(filename).isDirectory()) ){
					File f = new File(filename) ;
				try{
					 out = new PrintWriter(new FileWriter(f) ) ;
					 //----------------
					 // �� FileWriter ʵ�����������ļ���д������
					 out. print ("��txt�ļ���Ϊ��file"+unixtime+".txt"+"\r\n");
					 out.close();
					}catch (IOException e)
					{
						e.printStackTrace();
					}
				 }
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
	}

 
}