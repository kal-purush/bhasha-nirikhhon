public class MaxChunksToMakeSorted {
    public static int maxChunks(int a[]){
        int c=0;
        int max=0;
        for(int i=0;i<a.length;i++){
            // Isla matlab ki array ka value maximum kha tak ja sakta hai
            max=Math.max(a[i],max);
            if(i==max)
            c++;

        }
        return c;
    }
    public static void main(String[] args) {
        int a[]={2,0,1};
        int max=maxChunks(a);
        System.out.println("Max Chunks:::"+max);
    }
}