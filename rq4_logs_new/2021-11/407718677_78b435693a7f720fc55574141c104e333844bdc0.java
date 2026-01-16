public class Main {
    public static void main(String[] args) {

    }
}
//        Solution s = new Solution();
//        System.out.println(s.generateParenthesis(6));
        System.out.println(generateParenthesis(3));
    }
    public static List<String> generateParenthesis(int n) {
        List<String> res = new ArrayList<>();
        dfs("", n, n, res);
        return res;
    }
    /**
     * dfs + 回溯
     * 有效括号：左括号剩余数必须少于右括号时，才可以添加右括号
     * @param s  s的传递相当于回溯
     * @param lCount
     * @param rCount
     * @param res
     */
    public static void dfs (String s, int lCount, int rCount, List<String> res) {
        if (lCount == 0 && rCount == 0) {
            res.add(s);
            return;
        if (lCount > 0)
            dfs(s+"(", lCount-1, rCount, res);
        if (rCount > 0 && lCount < rCount)
            dfs(s + ")", lCount, rCount - 1, res);

//    static class Solution {
//        private int n;
//        private List<String> list;
//        private int[] visit;
//        private StringBuilder s;
//
//        public List<String> generateParenthesis(int n) {
//            this.n = n;
//            this.list = new ArrayList<>();
//            this.visit = new int[2*n+1];
//            this.s = new StringBuilder();
//
//            Set<String> sets = new HashSet<>();
//            for (int i = 1; i <= 2*n; i++) {
//                if (i <= n) {
//                    visit[i] = 1;
//                    s.append("(");
//                    dfs(s, sets);
//                    s.deleteCharAt(s.length()-1);
//                    visit[i] = 0;
//                } else {
//                    visit[i] = 1;
//                    s.append(")");
//                    dfs(s, sets);
//                    s.deleteCharAt(s.length()-1);
//                    visit[i] = 0;
//                }
//
//            }
//
//            this.list = new ArrayList<>(sets);
//
//            int j = 0;
//            while (j != list.size()) {
//                if (!isValid(list.get(j)))
//                    list.remove(j);
//                else
//                    j++;
//            }
//
//            return this.list;
//        }
//
//        /**
//         * dfs + 回溯生成所有情形
//         * @param s
//         */
//        public void dfs(StringBuilder s, Set<String> sets) {
//            /**
//             * 满足条件
//             */
//            if (s.length() == 2*n) {
//                sets.add(new String(s));
//                return;
//            }
//
//            for (int i = 1; i <= 2*n; i++) {
//                /**
//                 * 剪枝
//                 */
//                if(visit[i] == 1){
//                    continue;
//                }
//
//                if (i <= n) {
//                    visit[i] = 1;
//                    s.append("(");
//                    dfs(s, sets);
//                    s.deleteCharAt(s.length()-1);
//                    visit[i] = 0;
//                } else {
//                    visit[i] = 1;
//                    s.append(")");
//                    dfs(s, sets);
//                    s.deleteCharAt(s.length()-1);
//                    visit[i] = 0;
//                }
//            }
//        }
//
//        /**
//         * 判断是否是有效括号
//         * @param s
//         * @return
//         */
//        public boolean isValid(String s) {
//            Stack<Character> stack = new Stack<Character>();
//
//            for (int i = 0; i < s.length(); i++) {
//                if (s.charAt(i) == '(') {
//                    stack.push('(');
//                } else {
//                    if (stack.empty() || stack.pop() != '(') {
//                        return false;
//                    }
//                }
//            }
//
//            if (!stack.empty()) {
//                return false;
//            }
//
//            return true;
//        }
//    }