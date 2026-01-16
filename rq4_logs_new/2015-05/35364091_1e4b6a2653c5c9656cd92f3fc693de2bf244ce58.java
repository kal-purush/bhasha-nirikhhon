package com.ims.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Environment;

public class ImageStorageUtil {
	
	/** 最大保存文件数，多于会删除最旧的*/
	public static int MAX_FILE = 50;
	/**
	 * 纯
	 * @return
	 */
	public static String getRoolFile(){
		File _root = new File(Environment.getExternalStorageDirectory().getAbsolutePath() + File.separator + "IMS");
		if (!_root.exists()){
			_root.mkdir();
		}
		return _root.getAbsolutePath();
	}
	/**
	 * 保存图片到sd卡
	 * @param bmp
	 * @return
	 */
	public static boolean saveBitmap(Bitmap bmp){
		FileOutputStream out = null;
		try {
			String _fileName = ImageStorageUtil.getRoolFile() + "/" +System.currentTimeMillis() + ".png";
		    out = new FileOutputStream(_fileName);
		    bmp.compress(Bitmap.CompressFormat.PNG, 40, out); // bmp is your Bitmap instance
		    // PNG is a lossless format, the compression factor (100) is ignored
		    ImageStorageUtil.clean();
		    return true;
		} catch (Exception e) {
		    e.printStackTrace();
		    return false;
		} finally {
		    try {
		        if (out != null) {
		            out.close();
		        }
		    } catch (IOException e) {
		        e.printStackTrace();
		    }
		}
	}
	/**
	 * 保存图片数目若超过MAX_FILE，删除最旧的
	 */
	public static void clean(){
		File root_file = new File(getRoolFile());
		int files_count = root_file.listFiles().length;
		if(files_count > MAX_FILE){
			for(File f : root_file.listFiles()){
				f.delete();
				files_count--;
				System.out.println("清理到"+files_count);
				if(files_count <= MAX_FILE){
					break;
				}
			}
		}
	}
	/**
	 * 清空所有保存的图片
	 */
	public static void cleanAllFile(){
		File root_file = new File(getRoolFile());
		for(File f : root_file.listFiles()){
			f.delete();
		}
	}
	/**
	 * 获得图片保存列表中的图片bitmap
	 * @param index
	 * @return
	 */
	public static Bitmap getBitmap(int index){
		System.out.println("get bitmap:" + index);
		File root_file = new File(getRoolFile());
		int files_count = root_file.listFiles().length;
		if(files_count == 0){
			return null;
		}
		if(index >= files_count || index < 0){
			index = files_count - 1;
		}
		File target = root_file.listFiles()[index];
		return BitmapFactory.decodeFile(target.getAbsolutePath());
	}
	/**
	 * 获得保存图片文件的总数
	 * @return
	 */
	public static int getFileCount(){
		return new File(getRoolFile()).listFiles().length;
	}
}