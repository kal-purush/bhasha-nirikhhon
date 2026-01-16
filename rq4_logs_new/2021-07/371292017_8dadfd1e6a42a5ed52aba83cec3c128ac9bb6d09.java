package Stack;

import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;

/**
 * @Author ken_xing
 * @Date 2021/7/9--15:25
 * @Version 1.0
 * @Description 有效的括号
 */
public class ValidParentheses {
    public boolean isValidMethod1(String s)
    {
        if (s.length()%2!=0)
        {
            return false;
        }
        else
        {
            int loopNum=s.length()/2;
            for (int i=0;i<loopNum;i++)
            {
                s.replace("()","").replace("[]","").replace("{}","");
            }
            if (s.length()==0)
                return true;
            else
                return false;
        }
    }

    public boolean isValidMethod2(String s)
    {
        if (s.length()%2!=0)
        {
            return false;
        }
        else
        {
            Dictionary<Character,Character> dic=new Dictionary<Character, Character>() {
                @Override
                public int size() {
                    return 0;
                }

                @Override
                public boolean isEmpty() {
                    return false;
                }

                @Override
                public Enumeration<Character> keys() {
                    return null;
                }

                @Override
                public Enumeration<Character> elements() {
                    return null;
                }

                @Override
                public Character get(Object key) {
                    return null;
                }

                @Override
                public Character put(Character key, Character value) {
                    return null;
                }

                @Override
                public Character remove(Object key) {
                    return null;
                }
            };

            for (int i=0;i<s.length();i++) {

            }

            return true;

        }
    }
}