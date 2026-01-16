// this is a search suggestion system leetcode program.
//Question: Given an array of strings products and a string searchWord. We want to design a system that suggests at most three product names from products after each character of searchWord is typed
// the search keyword and product item must have common prefix and should be displayed lexographically.


//Solution: I used brute force approach. for each subsequence(0,i+1) from searchWord, I iterated over the list of products and
//loaded them into heap lexographically providing they have common prefix.
// then loaded into result set. by polling three items from heap for every subsequence.
//TimeComplexity: O(s(p+s)) : s: searchWord length and p : products length.
class SearchSuggestionSystem{
    public List<List<String>> suggestedProducts(String[] products, String searchWord) {

        if(searchWord == null || products.length == 0)
            return new ArrayList();
        List<List<String>> result = new ArrayList();
        for(int i =0; i<searchWord.length(); i++){
            String subsequence = searchWord.substring(0,i+1);
            PriorityQueue<String> heap = new PriorityQueue<String>((a,b)->a.compareTo(b));
            List<String> list = new ArrayList();
            int k =3;
            for(String product : products){
                if(!(i+1> product.length()) &&product.substring(0,i+1).equals(subsequence)){
                    heap.add(product);
                }
            }
            while(!heap.isEmpty() && k-->0){
                list.add(heap.poll());
            }
            result.add(list);
        }
        return result;
    }
}
