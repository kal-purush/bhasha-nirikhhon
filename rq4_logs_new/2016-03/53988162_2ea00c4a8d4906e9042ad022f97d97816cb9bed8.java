public class Cetakan {
    
    public String jenis;
    public int jml;
    static int  total;
    
    Cetakan (){
        jenis = "Brownies";
        jml = 100;
        total = 100;
        
    }
    Cetakan (String a, int b){
        jenis = a;
        jml = b;
        total += b;
                
    }
    public void setjenis(String a){
        jenis = a;
    }
    public void setjml(int b){
        jml = b;
    }
        
   public void penjumlahan(Cetakan x){
       System.out.println("Jumlah penjualan "+jenis+" hari ini : "+jml);
       System.out.println("Jumlah penjualan "+x.jenis+" hari ini : "+x.jml);
       System.out.println("Jumlah penjualan hari ini :"+total);
       
       
   }
    
            
    
}