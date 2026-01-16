/**
 * @author xicoll6
 * @Description: 报数
 * @date 2019/12/2216:39
 */
public class Solution38 {
    public String countAndSay(int n) {
        String pre = "1";
        for(int i=1; i<n; i++) {
            StringBuilder temp = new StringBuilder();
            char c = pre.charAt(0);
            int cnt = 1;
            for(int j=1; j<pre.length(); j++) {
                char cc = pre.charAt(j);
                if(c == cc) {
                    cnt++;
                } else {
                    temp.append(cnt).append(c);
                    cnt = 1;
                    c = cc;
                }
            }
            temp.append(cnt).append(c);
            pre = temp.toString();
        }
        return pre;
    }
}

