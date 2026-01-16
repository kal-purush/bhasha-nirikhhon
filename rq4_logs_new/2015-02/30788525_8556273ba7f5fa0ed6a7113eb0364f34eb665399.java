package com.common.tools;

import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Hashtable;
import java.util.Map;

import javax.imageio.ImageIO;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.BinaryBitmap;
import com.google.zxing.DecodeHintType;
import com.google.zxing.LuminanceSource;
import com.google.zxing.MultiFormatReader;
import com.google.zxing.MultiFormatWriter;
import com.google.zxing.Result;
import com.google.zxing.client.j2se.BufferedImageLuminanceSource;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.common.HybridBinarizer;

/**
 * 二维码工具,可以生成及解码二维码图
 * @author hyq
 *
 */
public class QrCode {
	/**
	 * 根据内容生成二维码图保存到指定的路径；
	 * @param content 二维码的内容
	 * @param imageFile 生成二维码图要保存到的完整（包括路径）文件名；
	 * @throws Exception
	 */
	public static void encode(String content, String imageFile) throws Exception {
		BitMatrix byteMatrix = new MultiFormatWriter().encode(content,BarcodeFormat.QR_CODE, 350, 350);
		MatrixToImageWriter.writeToFile(byteMatrix, "png", new File(imageFile));
	}

	/**
	 * 根据给定二维码图，解码其中的文本内容；
	 * @param imageFile 二维码图的完整（包括路径）文件名；
	 * @return 文本内容
	 * @throws Exception
	 */
	public static String uncode(String imageFile) throws Exception {
		BufferedImage image = ImageIO.read( new File(imageFile));
		if (image == null) {
			throw new RuntimeException("二维码图不存在");
		}
		LuminanceSource source = new BufferedImageLuminanceSource(image);
		BinaryBitmap bitmap = new BinaryBitmap(new HybridBinarizer(source));

		Map<DecodeHintType, Object> hints = new Hashtable<DecodeHintType, Object>();
		hints.put(DecodeHintType.CHARACTER_SET, "UTF-8");

		Result result = new MultiFormatReader().decode(bitmap, hints);
		return result.getText();
	}

	public static void main(String[] args) {
		try {
			//encode("test.test.test", new File("c:\\test.png"));
			System.out.println(uncode("c:\\test.png"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}