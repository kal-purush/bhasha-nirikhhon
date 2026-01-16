package Classes;
import java.util.ArrayList;

public class TourPackage {
    
     private String PackageName;

     private String District;

     private ArrayList<String>SpotName;

     private ArrayList<Integer>SpotPrice;

     private int TotalPrice=0;

     public TourPackage()
     {
        PackageName=null;
        District=null;
        SpotName=null;
        SpotPrice=null;
        TotalPrice=0;
     }

     public TourPackage(String PackageName,String District, ArrayList<String>SpotName, ArrayList<Integer> SpotPrice)
     {
        this.PackageName=PackageName;
        this.District = District;
        this.SpotName =SpotName;
        this.SpotPrice = SpotPrice;

        for(var it:SpotPrice)
        {
            TotalPrice+=it;
        }
     }

     public void setPackageName(String PackageName)
     {
        this.PackageName = PackageName;
     }

     public void setDistrict(String District)
     {
        this.District = District;
     }

     public void setSpotName(ArrayList<String>SpotName)
     {
        this.SpotName = SpotName;
     }

     public void setSpotPrice(ArrayList<Integer>SpotPrice)
     {
        this.SpotPrice = SpotPrice;
     }

     public void setTotalPrice()
     {
        TotalPrice=0;
        for(var it:SpotPrice)
        {
            TotalPrice+=it;
        }
     }

     public String getPackageName()
     {
       return PackageName;
     }

     public String getDistrict()
     {
        return District;
     }

     public ArrayList<String> getSpotName()
     {
        return SpotName;
     }

     public ArrayList<Integer> getSpotPrice(ArrayList<Integer>SpotPrice)
     {
        return SpotPrice;
     }

     public int getTotalPrice()
     {
        return TotalPrice;
     }
}