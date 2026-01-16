package com.wisn.tools;


import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;

public class FSUtils {

	/**
	 * 两级目录可以存256个，可以任意添加目录
	 * /*
	 如果文件上传到同一个文件夹下会很卡
	 采用的方式有以下：
	 1日期和时间分离  一个月，一个周，一天，几个小时
	 2按用户进行分离 一个用户一个文件夹
	 3按个数进行分离  一个文件夹下存放3000个超过在创建一个
	 4使用目录分离的算法:
	 将一个唯一的文件名。字符串类型，得到一个hashCode 32位，使用1111，分别和后面的4位与运算
	 每级作为一个目录  共8级   每级可以有16个，16的8次方约40亿个

	 * 
	 * @param name
	 * @return
	 */
	public static String getPath(String name) {
		int code1 = name.hashCode();
		int d1 = code1 & 15;
		int code2 = (code1 >>> 4) & 15;
		int d2 = code2 & 15;
		return File.separator+d1 +  File.separator + d2+ File.separator;
	}

	public static String getUUID() {
		return UUID.randomUUID().toString().replace("-", "");
	}

	public static void main(String[] args) throws IOException {
//		filePathTest("E\\backup");
		/*for( int i=0;i<1000;i++){
			String fileName = getFileName("a" + i);
			String path=getPath(fileName)+"/"+fileName;
			System.out.println(" path:"+path);
		}*/

	}
	/**
	 * 深度优先遍历
	 * 
	 * @param filename
	 */
	public static void fileListDeep(String filename) {
		// 创建一个列队
		Queue<File> queue = new LinkedList<File>();
		// 将根节点入队
		File root = new File(filename);
		queue.offer(root);// 入队
		while (!queue.isEmpty()) {
			File file = queue.poll();// 出队
			File[] files = file.listFiles();
			for (File f : files) {
				if (f.isFile()) {// 是一个文件
					// 找到了叶子节点
					System.out.println(f.getName());
				} else {
					// 有子节点
					queue.offer(f);// 入队
				}
			}
		}
	}
	/**
	 * 广度优先遍历
	 * @param filename
	 */
	public static  void fileListScope(String  filename){
		
	}
}