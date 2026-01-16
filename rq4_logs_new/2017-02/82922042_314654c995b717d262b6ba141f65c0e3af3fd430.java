package Guangyu;
import java.util.HashSet;  
import java.util.Set;  

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class StrLength {
	
	/**
    * @param 穿进去的字符串
    * @return 返回字符串的长度
    */
	    public static int maxLength(String str){  
	    	
	    	//首先判断输入的字符串是否为空
	        if(StringUtils.isEmpty(str)){  
	            return 0;  
	        }  
	        Set<Character> set=new HashSet<Character>();  
	        int maxLength=0;  
	        int pre=0;  
	        int after=0;  
	        
	        //当字符创不为空时
	        while(after < str.length()){  
	        	
	        	//set中你不存在该字符时加入进去
	            if( ! set.contains(str.charAt( after ) )){  
	                set.add( str.charAt( after ) );  
	                after++;  
	                
	             //set中存在该字符时
	            }else{  
	                set.clear();   
	                if( ( after-pre ) > maxLength ){  
	                    maxLength = after-pre;  
	                }  
	                pre++;  
	                after = pre;  
	            }  
	        }  
	          
	        if((after-pre)>maxLength){  
	            maxLength=after-pre;  
	        }  
	        return maxLength;  
	    }  
}  
