package simple_8583;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.log4j.Logger;

import simple_8583.factory.IsoMsgFactory;
import simple_8583.factory.XmlReader;
import simple_8583.model.IsoPackage;
import simple_8583.util.EncodeUtil;
import simple_8583.util.MacUtil;




/**
 * <p>交行核销主要方法api</p>
 * 主要方法API
 * @author zk
 * 2016-4-9 
 * 
 */
public class InterfaceAPI {
	
	static Logger log = Logger.getLogger(InterfaceAPI.class);
	
	/**
	 * 根据配置文件获得8583报文结构
	 * @param requestMap
	 * @param configUrl
	 * @return pack
	 */
	public static IsoPackage getPackage (Map<String,String> requestMap,String configUrl){
		XmlReader xmlReader;
		IsoPackage pack = null;
		byte[] sendData = null;
		// 读取配置文件
		try {
			xmlReader = new XmlReader(configUrl);
			String mti = requestMap.get("mti");
			pack = xmlReader.getIsoConfig().get(mti);
		} catch (JAXBException e) {
			log.info("exception "+e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			log.info("exception "+e.getMessage());
			e.printStackTrace();
		}

		return pack;
    }
	
	 /**
     * 读取配置文件组织报文
     * @param requestMap
     * @param configUrl 要解析的文件路劲
     * @param macKey 密钥
     * @return sendData
     */
    public static byte[] getPackets (Map<String,String> requestMap,String macKey,String configUrl){
		XmlReader xmlReader;
		byte[] sendData = null;
		// 读取配置文件
		try {
			// 单例
			IsoMsgFactory factory = IsoMsgFactory.getInstance();// 初始化工厂
			factory.setMacKey(macKey);// 设置签名mac秘钥
			IsoPackage pack = getPackage(requestMap,configUrl);
			sendData = factory.pack(requestMap, pack);// 需要发送的报文字节流
		} catch (IOException e) {
			log.info("exception "+e.getMessage());
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			log.info("exception "+e.getMessage());
			e.printStackTrace();
		}

		return sendData;
    }
    
    /**
     * 发送报文并获取返回信息
     * @param sendData 发送的报文
     * @return Map
     */
    public static byte[] sendPackets(byte[] sendData,String ip,int port,int timeout){
    	byte[] buf = null;
		byte[] all = null;
		try {
	    	Socket socket = new Socket();
			InetAddress inetAddress;
			inetAddress = InetAddress.getByName(ip);
			InetSocketAddress address = new InetSocketAddress(inetAddress, port);
			try {
				socket.setReuseAddress(true);
				socket.connect(address);
				socket.setSoTimeout(timeout);
				//--发送报文信息
				socket.getOutputStream().write(sendData);
				socket.getOutputStream().flush();
				byte[] lenbuf = new byte[3];
				
				//--获取返回信息
				while (socket != null && socket.isConnected()) {
					if (socket.getInputStream().read(lenbuf) == 3) {
						//计算报文所表示的报文长度(第二第三字节)
						int size = computeLength(lenbuf)+2;
						//新建对应长度字节数组
						buf = new byte[size];
						//读取从第4个字节到最后的数据
						socket.getInputStream().read(buf);
						//拼接整个返回报文从头到尾 top一个字节，尾部一个字节，LRC一个字节，共3字节
						all = new byte[size+3];
						System.arraycopy(lenbuf, 0, all, 0, 3);//数据包包头和包长度，复制到1-3位
						System.arraycopy(buf,0, all, 3, buf.length);//把数据放到4-leng+4
						break;
					}
				}
				
			} catch (SocketException e) {
				log.info("exception "+e.getMessage());
				e.printStackTrace();
			} catch (IOException e) {
				log.info("exception "+e.getMessage());
				e.printStackTrace();
			}
			
		} catch (UnknownHostException e) {
			log.info("exception "+e.getMessage());
			e.printStackTrace();
		}
		
		return all;
		
    }
    
    
   /**
    * 解析返回报文 
    * @param back
    * @param requestMap
    * @param configUrl
    * @return
    */
   public static Map<String,String> resolve(byte[] back,Map<String,String> requestMap,String configUrl){
	   Map<String,String> map = new HashMap<String, String>();
	   IsoPackage pack = getPackage(requestMap,configUrl);//根据配置文件和参数得到报文的数据结构
	   pack.remove(0);//将前面的MsgLength域剔除
	   try {
		IsoMsgFactory factory = IsoMsgFactory.getInstance();// 初始化工厂
		map = factory.unpack(back, pack);//工厂类的解析方法
	} catch (Exception e) {
		log.info("exception "+e.getMessage());
		e.printStackTrace();
	}
	   return map;
	   
   }
    
    
    
    
    
    
    
    
 /**************************************************************************************************************************/
    
  /**
   * 获取报文长度信息（根据不同的报文要求重写该方法）
   * 此方法目前是针对长度信息在第2和3个字节
   * @param lenBts
   * @return
   */
    public static int computeLength(byte[] lenBts){
    	if(lenBts.length!=3){
			throw new IllegalArgumentException("字节长度不正确，预期值为3，实际值为："+lenBts.length);
		}
		byte[] len = new byte[2];
		len[0]=lenBts[1];
		len[1]=lenBts[2];
		return Integer.valueOf(EncodeUtil.hex(len));
    }
    
    /**
	 * 解密63域密钥包 此方法只针对交行核销接口
	 * 说明：1，密钥重置接口会返回加密的63域，此域包涵3个密钥，返回报文时24位16进制数（一位长度代表报文2数字位）
	 *     先要取得此报文将报文按8位长度分隔成3块分别对应新主密钥，work密钥，新MAC密钥，然后先去第一个8位
	 *     DES解密，解密出来的新主密钥用来分别给work密钥和新MAC密钥解密用的
	 *     2，签到接口会返回加密的63域，此域包涵2个密钥，共16位16进制数。
	 * @param str 
	 * @param key 解密的key
	 * @return
	 */
	public static  Map decryptionDES(String str,String key){
		Map<String,String> map = new HashMap<String,String>();
		//适用于2个接口，返回的长度也有2种
		if(str.length()/2==24 || str.length()/2==16){
			if(str.length()/2==24){
				//将字符串按8位分隔
				String master = str.substring(0,16);
				String work = str.substring(16,32);
				String newMac = str.substring(32,48);
				//先解密第一个8位,得到主密钥
				String masterKey = MacUtil.DES_1(master,key,1);
				//用主密钥去解其他2个密钥
				String workKey = MacUtil.DES_1(work,masterKey,1);
				String newMacKey = MacUtil.DES_1(newMac,masterKey,1);
				map.put("masterKey", masterKey);
				map.put("workKey", workKey);
				map.put("newMacKey", newMacKey);
			}
			if(str.length()/2==16){
				//将字符串按8位分隔
				String workKey = str.substring(0,16);
				String newMac = str.substring(16,32);
				//用主密钥去解其他2个密钥
				String masterKey = MacUtil.DES_1(workKey,key,1);
				String newMacKey = MacUtil.DES_1(newMac,key,1);
				map.put("workKey", workKey);
				map.put("newMacKey", newMacKey);
			}
		}
		
		return map;
	} 
}