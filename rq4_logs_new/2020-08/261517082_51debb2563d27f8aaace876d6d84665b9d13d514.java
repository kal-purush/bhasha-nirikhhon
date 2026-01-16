/*
        Design an Iterator class, which has:
        A constructor that takes a string characters of sorted distinct lowercase English letters and a number combinationLength as arguments.
        A function next() that returns the next combination of length combinationLength in lexicographical order.
        A function hasNext() that returns True if and only if there exists a next combination.

        Example:
        CombinationIterator iterator = new CombinationIterator("abc", 2); // creates the iterator.
        iterator.next(); // returns "ab"
        iterator.hasNext(); // returns true
        iterator.next(); // returns "ac"
        iterator.hasNext(); // returns true
        iterator.next(); // returns "bc"
        iterator.hasNext(); // returns false
        

        Constraints:
        1 <= combinationLength <= characters.length <= 15
        There will be at most 10^4 function calls per test.
        It's guaranteed that all calls of the function next are valid.
*/
class CombinationIterator {

        Queue<String> queue;
        public CombinationIterator(String characters, int combinationLength) {
                queue = new LinkedList();
                generateCombination(characters.toCharArray(), 0, new StringBuilder(), combinationLength, queue);
        }
        
        private void generateCombination(char[] chars, int index, StringBuilder str, int k, Queue<String> queue){
                if(k == 0){
                        queue.add(str.toString());
                        return;
                }
                
                for(int i = index; i < chars.length; i++){
                        str.append(chars[i]);
                        generateCombination(chars, i+1, str, k-1, queue);
                        str.deleteCharAt(str.length()-1);
                }
        }
        
        public String next() {
                return queue.poll();
        }
        
        public boolean hasNext() {
                return !queue.isEmpty();
        }
}
    
    /**
     * Your CombinationIterator object will be instantiated and called as such:
     * CombinationIterator obj = new CombinationIterator(characters, combinationLength);
     * String param_1 = obj.next();
     * boolean param_2 = obj.hasNext();
     */