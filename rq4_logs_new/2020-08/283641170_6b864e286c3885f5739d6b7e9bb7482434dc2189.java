/**
 * // This is the interface that allows for creating nested lists.
 * // You should not implement it, or speculate about its implementation
 * public interface NestedInteger {
 *
 *     // @return true if this NestedInteger holds a single integer, rather than a nested list.
 *     public boolean isInteger();
 *
 *     // @return the single integer that this NestedInteger holds, if it holds a single integer
 *     // Return null if this NestedInteger holds a nested list
 *     public Integer getInteger();
 *
 *     // @return the nested list that this NestedInteger holds, if it holds a nested list
 *     // Return null if this NestedInteger holds a single integer
 *     public List<NestedInteger> getList();
 * }
 */
public class NestedIterator implements Iterator<Integer> {
    private List<Integer> list;
    private int i =0;
    public NestedIterator(List<NestedInteger> nestedList) {
        list = new ArrayList<>();
        flattenList(nestedList);
    }
    public void flattenList(List<NestedInteger> nestedList) {
        for(NestedInteger val : nestedList){
            if(val.isInteger() == true) {
                list.add(val.getInteger());
            }
            else {
                flattenList(val.getList());
            }
        }
    }

    @Override
    public Integer next() {
        return list.get(i++);
    }

    @Override
    public boolean hasNext() {
        if(i == list.size())
            return false;
        else
            return true;
    }
}

/**
 * Your NestedIterator object will be instantiated and called as such:
 * NestedIterator i = new NestedIterator(nestedList);
 * while (i.hasNext()) v[f()] = i.next();
 */

// this question gives a data list containing nested lists and integer. the output should be a list of integer after flattening list of lists
// use recursion with base case as the value is integer and add to output list. other wise recurse that list.
// time complextiy : O(n) where n is the number of integer.
// space complexity : O(NL) max depth of nested list.