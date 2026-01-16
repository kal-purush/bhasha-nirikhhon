package src.my.com.dfs;

import java.util.*;
// Accounts Merge
// Given a list of accounts where each element accounts[i] is a list of strings, where the first element accounts[i][0] is a name, and the rest of the elements are emails representing emails of the account.
// We need to merge accounts that have common emails. The merged account should contain the name and all unique emails sorted in lexicographical order.


// Approach: 
// 1. Create an adjacency list to represent the graph where each email is a node and edges represent common emails between accounts.
// 2. Use DFS to traverse the graph and collect all connected emails.
// 3. For each account, if the email has not been visited, perform DFS to find all connected emails, sort them, and add the name at the beginning.  
// 4. Return the list of merged accounts.


class Solution {
    
    // since two accounts can belong to one person if the two account have a common email 
        // considering all unique emails belong to one person, we can create edges between all the emails , Thus all email connected with some edge will belong to same person. Emails can belong to any account.

    HashMap<String , List<String> > graph = new HashMap();
    HashSet<String> visited  = new HashSet();

    public List<List<String>> accountsMerge(List<List<String>> accounts) {
    
        // create adjacency list
        for ( List<String> sl : accounts ){

            if ( !graph.containsKey( sl.get(1))){
                graph.put( sl.get(1) , new ArrayList());
            }

            for ( int i=2;i < sl.size(); i++){
                graph.get(sl.get(1)).add( sl.get(i) );
                
                List<String> l = graph.getOrDefault( sl.get(i), new ArrayList());
                l.add( sl.get(1) );
                graph.put( sl.get(i), l );
            }
        }

        List<List<String>> mergedAccounts = new ArrayList();
        for ( List<String> sl : accounts ){
            
            List<String> ls = new ArrayList();
            if ( !visited.contains(sl.get(1))){
                
                
                dfs(ls , sl.get(1));
                Collections.sort(ls);
                ls.add(0, sl.get(0));
                mergedAccounts.add(ls);
            }
        }

        return mergedAccounts;
    }

    public void dfs ( List<String> mergedAccount, String  str  ){
        
        visited.add(str);
        mergedAccount.add(str);
        
        for ( String neighbours : graph.get(str)){
            
            if ( !visited.contains(neighbours)){
                dfs(mergedAccount ,neighbours );
            }
        }

    } 
}