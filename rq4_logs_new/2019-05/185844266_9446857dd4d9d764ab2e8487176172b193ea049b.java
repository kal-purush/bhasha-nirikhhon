package com.signal;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 作者：administrator
 * 时间：2019年5月9日 下午9:46:44
 * 说明：面试题：HOMEWORK无人机
 */
public class Signal {

	public static void main(String[] args) throws IOException {
		String filePath = System.getProperty("user.dir") + "\\src\\signal.txt";
		if(filePath.endsWith(".txt")) {
			List<Map<String, Object>> transferFile = transferFile(filePath);
			inputAndShowResult(transferFile);
		} else {
			System.out.println("传入的文件不是TXT文件，请重新输入...");
			return;
		}
	}

	/**
	 * @author Administrator
	 * @time 2019年5月9日 下午9:49:09
	 * @used 用途：将输入的文件转换为需要用到的ListMap
	 * @param filePath
	 * @return
	 * @throws IOException 
	 */
	private static List<Map<String,Object>> transferFile(String filePath) throws IOException {
		printFileContext(filePath);
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		List<Map<String,Object>> listMap = new ArrayList<Map<String,Object>>();
		String str = br.readLine();
		String SignalId = null;
		int currLineId = 0;
		boolean status = true;
		while(str!=null)//如果当前行不为空
		{
			String[] lineInfo = str.trim().split(" ");
			if(listMap.size()==0 && lineInfo.length!=4) {
				status = false;
			}
			Map<String,Object> lineMap = new HashMap<String, Object>();
			lineMap.put("lineId", currLineId);
			String lineSignalId = lineInfo[0];
			if(SignalId == null) {
				SignalId = lineSignalId;
			} else {
				if(!SignalId.equals(lineSignalId)) {
					status = false;
				}
			}
			lineMap.put("SignalId", SignalId);
			if(status) {
				boolean matches = lineSignalId.matches("[\\da-zA-z]+");//正则校验无人机ID由数字和英文字母组成
				if(matches) {
					try {
						int X = Integer.parseInt(lineInfo[1]);
						int Y = Integer.parseInt(lineInfo[2]);
						int Z = Integer.parseInt(lineInfo[3]);
						lineMap.put("X", X);
						lineMap.put("Y", Y);
						lineMap.put("Z", Z);
						if(listMap.size()!=0) {
							if(lineInfo.length>5) {
								int offsetX = Integer.parseInt(lineInfo[4]);
								int offsetY = Integer.parseInt(lineInfo[5]);
								int offsetZ = Integer.parseInt(lineInfo[6]);
								lineMap.put("offsetX", offsetX);
								lineMap.put("offsetY", offsetY);
								lineMap.put("offsetZ", offsetZ);
							} else {
								status = false;
								lineMap.put("status", status);
							}
						}
						lineMap.put("status", status);
					} catch (NumberFormatException e) {
						status = false;
						lineMap.put("status", status);
					}
					if(listMap.size() > 2) {
						Map<String, Object> lastMap = listMap.get(listMap.size()-1);
						int x = (int) lastMap.get("X");
						int y = (int) lastMap.get("Y");
						int z = (int) lastMap.get("Z");
						int ox = (int) lastMap.get("offsetX");
						int oy = (int) lastMap.get("offsetY");
						int oz = (int) lastMap.get("offsetZ");
						int X = (int) lineMap.get("X");
						int Y = (int) lineMap.get("Y");
						int Z = (int) lineMap.get("Z");
						if(x+ox!=X || y+oy!=Y || z+oz!=Z) {
							status = false;
							lineMap.put("status", status);
						}
					}
				} else {
					status = false;
					lineMap.put("status", status);
				}
			} else {
				lineMap.put("status", status);
			}
			listMap.add(lineMap);
			currLineId ++ ;
			str= br.readLine();//读取下一行
		}
		br.close();
		return listMap;
	}

	/**
	 * @author Administrator
	 * @time 2019年5月9日 下午9:50:37
	 * @used 用途：將輸入的文件顯示在控制台
	 * @param br
	 * @throws IOException
	 */
	private static void printFileContext(String filePath) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String str = br.readLine();
		System.out.println("输入文件的内容是：\n====================================");
		while(str!=null)//如果当前行不为空
		{
			System.out.println("	"+str);//打印当前行
			str= br.readLine();//读取下一行
		}
		System.out.println("====================================");
		br.close();
	}
	
	/**
	 * @author Administrator
	 * @time 2019年5月9日 下午10:12:45
	 * @used 用途：输入想要查看的无人机序号，显示结果
	 * @param transferFile
	 */
	private static void inputAndShowResult(List<Map<String, Object>> transferFileList) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String str = null;
		int inputInt = -1;
		System.out.println("请输入正确的要查询的无人机消息序号：");
		System.out.println("输入 \"end\" 结束查询。");
		do {
			System.out.println("请输入：");
			try {
				str = br.readLine();
			} catch (IOException e1) {
				System.out.println("输入遇到错误，请重启。");
			}
			if(!"end".equals(str)) {
				try {
					inputInt = Integer.parseInt(str);
					if(inputInt < 0) {
						System.out.println("请输入大于零的数。");
						continue;
					}
					if(inputInt < transferFileList.size()) {
						Map<String, Object> map = transferFileList.get(inputInt);
						if(true==(boolean)map.get("status")) {
							int x = (int) map.get("X");
							int y = (int) map.get("Y");
							int z = (int) map.get("Z");
							if(inputInt > 0) {
								int ox = (int) map.get("offsetX");
								int oy = (int) map.get("offsetY");
								int oz = (int) map.get("offsetZ");
								System.out.println(map.get("SignalId") + " " + map.get("lineId") + " " + (x+ox) + " " + (y+oy) + " " + (z+oz));
							} else {
								System.out.println(map.get("SignalId") + " " + map.get("lineId") + " " + x + " " + y + " " + z);
							}
						} else {
							System.out.println("Error：" + inputInt);
						}
					} else {
						System.out.println("Cannot find " + inputInt);
					}
				} catch (NumberFormatException e) {
					System.out.println("请输入正确的无人机消息序号：");
				}
			}
		} while (!str.equals("end"));
	}
}