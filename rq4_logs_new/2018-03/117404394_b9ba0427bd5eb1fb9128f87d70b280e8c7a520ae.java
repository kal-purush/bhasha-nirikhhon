/**
 * Copyright (C), 2011-2018, 微贷网.
 */
package wz.study.algorithm.KMP;

import java.util.HashMap;

/**
 * @author wangzhe 2018/1/25.
 */
public class Solution {
    public boolean wordPattern(String pattern, String str) {
        String[] strs = str.split(" ");
        HashMap<String, Integer> mapstr = new HashMap<String, Integer>();
        HashMap<Character, Integer> mapc = new HashMap<Character, Integer>();
        if(pattern.length() != strs.length) return false;
        for(Integer i= 0;i < pattern.length();++i){
            char tmpc = pattern.charAt(i);
            String tmpstr = strs[i];//下一行的异或操作，不仅可以针对数字还可以针对逻辑
            if((mapc.containsKey(tmpc) ^ mapstr.containsKey(tmpstr))
                    ||(mapc.containsKey(tmpc) && mapstr.containsKey(tmpstr) && mapc.get(tmpc) != mapstr.get(tmpstr)))
                return false;
            mapc.put(tmpc, i);//这个写法很巧妙的，算是变相的实现了上边的Put对比的那种写法
            mapstr.put(tmpstr, i);//注意一定要把这两句Put放在if语句的后边，如果放在前边那完全就坏了
        }
        return true;
    }

    public static void main(String[] args) {
        System.out.println(new Solution().wordPattern("abba","北京 杭州 北京 北京"));
    }
}