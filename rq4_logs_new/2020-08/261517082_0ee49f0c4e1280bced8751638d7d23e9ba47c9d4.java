/*
        Design a data structure that supports the following two operations:

        void addWord(word)
        bool search(word)
        search(word) can search a literal word or a regular expression string containing only letters a-z or .. A . means it can represent any one letter.

        Example:

        addWord("bad")
        addWord("dad")
        addWord("mad")
        search("pad") -> false
        search("bad") -> true
        search(".ad") -> true
        search("b..") -> true
        Note:
        You may assume that all words are consist of lowercase letters a-z.
*/
class WordDictionary {

        /** Initialize your data structure here. */
        private TrieNode root;
        public WordDictionary() {
            root = new TrieNode();
        }
        
        /** Adds a word into the data structure. */
        public void addWord(String word) {
                TrieNode temp = root;
                for(char ch : word.toCharArray()){
                        if(!temp.contains(ch)){
                                temp.add(ch);
                        }
                        temp = temp.get(ch);
                }
                temp.setIsWord(true);
        }
        
        /** Returns if the word is in the data structure. A word could contain the dot character '.' to represent any one letter. */
        public boolean search(String word) {
                TrieNode temp     = root;
                Queue<TrieNode> q = new LinkedList();
                q.add(temp);
                for(char ch : word.toCharArray()){
                        int size = q.size();
                        if(size == 0){
                                return false;
                        }
                        if(ch == '.'){
                                for(int i = 0 ; i < size; i++){
                                        TrieNode       node = q.poll();
                                        List<TrieNode> list = node.getAllNonNull();
                                        q.addAll(list);
                                }
                        } else {
                        // System.out.println("size :::"+size);
                                for(int i = 0; i < size; i++){
                                        TrieNode node = q.poll();
                                        if(node.contains(ch)){
                                                q.add(node.get(ch));
                                        }
                                }    
                        }
                        
                }
                // System.out.println("Q szie :"+q.size());
                int size = q.size();
                for(int i = 0; i < size; i++){
                        TrieNode node = q.poll();
                        if(node.getIsWord()){
                                return true;
                        }
                }
                return false;
        }
        
        class TrieNode{
                private TrieNode[] nodes;
                private boolean isWord;
                
                public TrieNode(){
                        this.nodes = new TrieNode[26];
                }
                
                public boolean contains(char ch){
                        return this.nodes[ch-'a'] != null;
                }
                
                public TrieNode get(char ch){
                        return this.nodes[ch-'a'];
                }
                
                public void add(char ch){
                        this.nodes[ch-'a'] = new TrieNode();
                }
                
                public void setIsWord(boolean isWord){
                        this.isWord = isWord;
                }
                
                public boolean getIsWord(){
                        return this.isWord;
                }
                
                public List<TrieNode> getAllNonNull(){
                        List<TrieNode> list = new LinkedList();
                        for(int i = 0; i < 26; i++){
                                if(this.nodes[i] != null){
                                        list.add(this.nodes[i]);
                                }
                        }
                        return list;
                }
        }
}
    
    
    
    /**
     * Your WordDictionary object will be instantiated and called as such:
     * WordDictionary obj = new WordDictionary();
     * obj.addWord(word);
     * boolean param_2 = obj.search(word);
     */