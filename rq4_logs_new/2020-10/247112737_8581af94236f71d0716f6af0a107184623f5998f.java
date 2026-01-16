class Solution {
    public List<String> invalidTransactions(String[] transactions) {
        
        List<String> ans = new ArrayList<>();
        
        for(int i = 0; i < transactions.length; i++) {
            
            if(ans.contains(transactions[i])) continue;
            
            String[] values = transactions[i].split(",");
                        
            if(Integer.parseInt(values[2]) > 1000) {
                
                if(!ans.contains(transactions[i])) 
                    ans.add(transactions[i]);
                
            } else {
            
                for(int j = 0; j < transactions.length; j++) { 
                    
                    if(transactions[i] != transactions[j]) {
			
                        String[] b = transactions[j].split(",");
                    
                        if(Math.abs(Integer.valueOf(values[1])-Integer.valueOf(b[1]))<=60 && b[0].equals(values[0]) && !b[3].equals(values[3])) {
                    
                            if(!ans.contains(transactions[i]))
                                ans.add(transactions[i]);
                            if(!ans.contains(transactions[j]))
                                ans.add(transactions[j]);
                        }
                    }
                }    
            }
        }
        
        return ans;
    }
}