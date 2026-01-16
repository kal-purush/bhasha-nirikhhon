public class SplitAlgo {

    public class Node {
        public String str1;
        public String str2;
        public String parent;

        Node(String str1, String str2, String parent) {
            this.str1 = str1;
            this.str2 = str2;
            this.parent = parent;
        }
    }

    static ArrayList<String> visandhi(String str, HashSet<String> dict, ArrayList<SandhiRule> rules) {
        Stack<Node> stack = new Stack<>();
        HashMap<String, Node> alreadySeen = new HashMap<>();

        stack.push(new Node(str, "", ""));
        while(1) {
            Node curStr = stack.pop();

            if (dict.contains(curStr.str1)) {
                return recurse(curStr, alreadySeen);
            }

            alreadySeen.add(curStr.str1, curStr);

            for (int i = 1; i < curStr.size()-1; i++) {
                for (SandhiRule rule: rules) {
                    if (ruleSatisfies(curStr.str1, rule, i)) {
                        String split[] = splitWord(curStr.str1, rule, i);

                        if (!dict.contains(split[0]))
                            continue;

                        if (alreadySeen.containsKey(split[1]))
                            continue;

                        Node newNode = new Node(split[1], split[0], curStr.str1);
                        stack.push(newNode);
                    }
                }
            }
        }
    }

    static ArrayList<String> recurse(Node node, HashMap<String, Node> alreadySeen) {
        ArrayList<String> result = new ArrayList<>();
        result.add(node.str1);
        String parent = "";
        do {
            Node parentNode = alreadySeen.get(node.parent);
            parent = parentNode.parent;
        } while (!parent.isEmpty());
        return result;
    }

    static boolean ruleSatisfies(String str, SandhiRule rule, int index) {
        //Finds out whether String can be split at given point using given rule
    }

    static String[] splitWord(String str, SandhiRule rule, int index) {
        //Splits String at given point using given rule and returns both resulting words
    }
}