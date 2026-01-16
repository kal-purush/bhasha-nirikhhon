package com.netease.picture.constant;

import java.util.Map;

import com.google.common.collect.Maps;
import com.netease.picture.enums.PictureEnum;

/** 
* @author bjchenyuan
* @version 2016年6月13日 下午4:26:35
*/
public class PictureTargetConstant
{
	private static Map<String, String> pathMap = Maps.newHashMap();

	static
	{
		PictureEnum[] enumList = PictureEnum.values();
		for (PictureEnum en : enumList)
		{
			pathMap.put(en.getName(), en.getPath());
		}

	}

	public static String getPath(String target)
	{
		if (pathMap.containsKey(target))
		{
			return pathMap.get(target);
		}
		return "/picture/data";
	}

}