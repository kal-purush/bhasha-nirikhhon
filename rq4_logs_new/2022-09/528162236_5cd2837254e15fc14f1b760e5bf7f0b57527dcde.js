function getWays(n, c) {
    // Write your code here
    var total = 0;

    for(var i=0;i<c.length;i++) {
    
        if(c[i]==n){
            total++;
        }
        else if(c[i]>n){
            continue;
        }
        else{
            var count=c[i];
            while(count<n) {
                count+=c[i];
                if(count=n){
                    total++;
                }
            }

        }
    }
    console.log(total);

}

getWays(3,[8,3,1,2]);