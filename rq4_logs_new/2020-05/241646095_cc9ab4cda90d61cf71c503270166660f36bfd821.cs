static int camelcase(string s) {

        int count = 1;

        for(int i=1 ; i<s.Length ; i++)
            if(s[i]>='A' && s[i]<='Z') count++;
        
        return count;

    }