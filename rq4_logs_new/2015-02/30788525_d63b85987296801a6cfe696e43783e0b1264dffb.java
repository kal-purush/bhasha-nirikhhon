package com.common.beanutil;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Set;

/**
 * 提供一个对bean的转换操作；Class类型的相关快捷操作；
 * 此类还不能使用
 * <br/>时间：2012-7-31
 * @author yuqing
 */
public class BeanUtil {
	
	public Object jsonToBean(String jsonStr){
		/*try {
			Set<String> names=jo.keySet();
		  for(String fn : names) {
			  PropertyDescriptor pd = new PropertyDescriptor(fn,CcoreAccountInfo.class);
			  Method setor = pd.getWriteMethod();//获得写方法
			  if(pd.get)
			  setor.invoke(custemer,jo.get(fn).toString());//因为知道是int类型的属性，所以传个int过去就是了。。实际情况中需要判断下他的参数类型
		  }
		} catch (Exception e) {
			e.printStackTrace();
		} */
		return null;
	}

}