package com.gxl.common.thread;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.HasAttributeFilter;
import org.htmlparser.filters.HasChildFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.tags.Bullet;
import org.htmlparser.tags.ImageTag;
import org.htmlparser.util.NodeList;

import com.gxl.biz.ThreadBiz;
import com.gxl.common.Cache;

/**
 * 虚拟会员自动获取任务
 * @author gongxl
 *
 */
public class VtlMbrTask {
	private static final String startIdBoy = "628468660";
	private static final String startIdGirl = "628468660";
	
	private List<String> mbrList;
	
	private static String getUrl(String id){
		return "http://www.youyuan.com/" + id + "-profile/";
	}
	private ThreadBiz threadBiz;
	private static Integer maxRecs = 1500;
	private Integer recs;
	private Integer doRecs;
	
	public VtlMbrTask(ThreadBiz threadBiz) throws Exception {
		this.threadBiz = threadBiz;
		this.recs = 0;
		this.doRecs = 0;
		if(Cache.waitIds.isEmpty()){
			Cache.waitIds.add(startIdBoy);
			Cache.waitIds.add(startIdGirl);
		}
		run();
	}
	private void run(){
		while(!Cache.waitIds.isEmpty()){
			try {
				Thread.currentThread();
				Thread.sleep(50);//睡眠防止卡死
				getPerInfo(Cache.waitIds.get(0));
				Cache.waitIds.remove(0);
				if(recs >= maxRecs){
					System.out.println("=========================正在批量录入"+recs+"条数据========================");
					threadBiz.txAddVtlMbr(mbrList);
					mbrList.clear();
					System.out.println("=========================录入成功========================");
					doRecs = doRecs + recs;
					recs = 0;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if(recs > 0){
			System.out.println("=========================正在批量录入"+recs+"条数据========================");
			try {
				threadBiz.txAddVtlMbr(mbrList);
			} catch (Exception e) {
				e.printStackTrace();
			}
			mbrList.clear();
			System.out.println("=========================全部录入成功========================");
			doRecs = doRecs + recs;
			System.out.println("=========================本次总共录入"+doRecs+"条数据========================");
			recs = 0;
		}
	}
	
	private static void getPerInfo(String id) throws Exception{
		String url = getUrl(id);
		System.out.println("******************************************开始获取会员（id："+id+"）信息******************************************");
		Parser parser = new Parser( (HttpURLConnection) (new URL(url)).openConnection());
		getNickName(parser);
		getPidUrl(parser,id);
		getLocalInfo(parser);
		getHoby(parser);
		getSay(parser);
		getDetail(parser);
		getIds(parser);
	}
	private static void getIds(Parser parser) throws Exception{
		parser.reset();
		NodeList list = parser.parse(getFilter("class|inPerson"));
		for (int i = 0; i < list.size(); i++) {
			Bullet tag = (Bullet)list.elementAt(i);
			String id = tag.getAttribute("data-kd");
			if(!Cache.vtlMbrIds.contains(id)){
				if(!Cache.waitIds.contains(id)){
					Cache.waitIds.add(id);
				}
			}
		}
	}
	private static void getPidUrl(Parser parser,String id) throws Exception{
		parser.reset();
		Node node = parser.parse(getFilter("class|personal_cen")).elementAt(0).getFirstChild();
		ImageTag tag = (ImageTag)node.getFirstChild();
		String src = tag.getImageURL();
		System.out.println("头像链接：" + src);
		download(src,id+".jpg","C:\\Users\\gongxl\\Desktop\\");
	}
	
	private static void download(String urlString, String filename,String savePath) throws Exception {
		// 构造URL
		URL url = new URL(urlString);
		// 打开连接
		URLConnection con = url.openConnection();
		//设置请求超时为5s
		con.setConnectTimeout(5*1000);
		// 输入流
		InputStream is = con.getInputStream();
		
		// 1K的数据缓冲
		byte[] bs = new byte[1024];
		// 读取到的数据长度
		int len;
		// 输出的文件流
		File sf=new File(savePath);
		if(!sf.exists()){
			sf.mkdirs();
		}
		OutputStream os = new FileOutputStream(sf.getPath()+"\\"+filename);
		// 开始读取
		while ((len = is.read(bs)) != -1) {
			os.write(bs, 0, len);
		}
		// 完毕，关闭所有链接
		os.close();
		is.close();
	}
	
	private static void getDetail(Parser parser) throws Exception{
		parser.reset();
		NodeFilter filter1 = getFilter("class|black");
		NodeFilter filter2 = new HasChildFilter(filter1);
		NodeList list = parser.parse(filter2);
		if(list == null)return;
		for (int i = 0; i < list.size(); i++) {
			Node node = list.elementAt(i).getFirstChild();
			Node node1 = node.getNextSibling().getFirstChild();
			System.out.println(node.getText().replaceAll(" ","")+node1.getText().replaceAll(" ",""));
		}
	}
	
	private static void getSay(Parser parser) throws Exception{
		parser.reset();
		Node node = parser.parse(getFilter("class|requre")).elementAt(0).getFirstChild().getLastChild().getFirstChild();
		System.out.println("内心独白：" + node.getText().trim());
	}
	
	private static void getHoby(Parser parser) throws Exception{
		parser.reset();
		NodeList list = parser.parse(getFilter("class|hoby")).elementAt(0).getChildren();
		if(list == null)return;
		for (int i = 0; i < list.size(); i++) {
			Node node = list.elementAt(i).getFirstChild();
			System.out.println("hoby"+(i+1)+"：" + node.getText().trim().replaceAll("&nbsp;", ""));
		}
	}
	
	private static void getLocalInfo(Parser parser) throws Exception{
		parser.reset();
		Node node = parser.parse(getFilter("class|local")).elementAt(0).getFirstChild();
		String list = node.getText();
		String [] arr = list.split("\\s+");
		System.out.println("所在地：" + arr[0]);
		System.out.println("年龄：" + arr[1]);
		System.out.println("身高：" + arr[2]);
		System.out.println("月收入：" + arr[3]);
		System.out.println("是否有房：" + arr[4]);
	}
	private static void getNickName(Parser parser) throws Exception{
		parser.reset();
		Node node = parser.parse(getFilter("div|class|main")).elementAt(0).getChildren().elementAt(1);
		System.out.println("昵称：" + node.getText());
	}
	
	private static NodeFilter getFilter(String param){
		String [] arr = param.split("\\|");
		if(arr.length == 1){
			return new TagNameFilter (arr[0]);
		}else if(arr.length == 2){
			return new HasAttributeFilter(arr[0],arr[1]);
		}else if(arr.length == 3){
			NodeFilter filter1 = new TagNameFilter (arr[0]);
			NodeFilter filter2 = new HasAttributeFilter(arr[1],arr[2]);
			return new AndFilter(filter1, filter2);
		}else{
			return null;
		}
	}
}