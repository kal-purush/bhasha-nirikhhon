
package serv.model;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.sql.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import serv.DB.priorizedListDAOimpl;

public class FillpriorizedList {
   
    /**
     *
     * @author Hatim
     */
   
   
public static ArrayList<priorizedListObject> sort_List(ArrayList<priorizedListObject> priorizedList) { // PRIORIZED LIST SORT
       
   
   // SORT BY PRIORITY
   
    int longueur=0;
    for (priorizedListObject o : priorizedList){
        longueur++;
    }
      boolean inversion=true;   
      do
          {
          inversion=false;

          for(int i=0;i<longueur-1;i++)
              {
              if( priorizedList.get(i).getId_prio() > priorizedList.get(i+1).getId_prio())
                  {
                  Collections.swap(priorizedList, i, i+1);
                  inversion=true;
                  }
              }
          longueur--;
           }
      while(inversion);
     
      // SORT BY DATE IF PRIORITY IS THE SAME
     
      longueur=0;
      for (priorizedListObject o : priorizedList){
          longueur++;
      }
       
      do
          {
          inversion=false;

          for(int i=0;i<longueur-1;i++)
              {
              if( priorizedList.get(i).getId_prio() == priorizedList.get(i+1).getId_prio() && priorizedList.get(i).getDate_occured().compareTo(priorizedList.get(i+1).getDate_occured()) > 0  )
                  {
                  Collections.swap(priorizedList, i, i+1);
                  inversion=true;
                  }
              }
          longueur--;
           }
      while(inversion);
     
     
      // PUT ELEMENT ON TOP IF DATE OLDER THAN 3 WEEKS
     
      longueur=0;
      for (priorizedListObject o : priorizedList){
          longueur++;
      }
     
      for(int i=0;i<longueur-1;i++){
       java.util.Date date = new java.util.Date();
          int noOfDays = 21; //i.e two weeks
          Calendar calendar = Calendar.getInstance();
          calendar.setTime(date);           
          calendar.add(Calendar.DAY_OF_YEAR, -noOfDays);
          Date dateSystem =  new Date(date.getTime());
          if( priorizedList.get(i).getDate_occured().compareTo(dateSystem) < 0  ){
             
          }
      }
    return priorizedList;
     
}

public static ArrayList<priorizedListObject> fill_List() throws ParseException{
    ArrayList<priorizedListObject> priorizedList= new ArrayList<>();
    SimpleDateFormat simpledateformat=new SimpleDateFormat("dd/MM/yyyy");
   
    // FILL PRIORIZED LIST
    priorizedListObject o1 = new priorizedListObject(1, 1, 0, "bollore", 5, new Date (simpledateformat.parse("10/06/2017").getTime()));
    priorizedList.add(o1);
    priorizedListObject o2 = new priorizedListObject(2, 2, 0, "bollore", 10, new Date (simpledateformat.parse("07/06/2017").getTime()));
    priorizedList.add(o2);
    priorizedListObject o3 = new priorizedListObject(3, 3, 0, "bollore", 7, new Date (simpledateformat.parse("05/06/2017").getTime()));
    priorizedList.add(o3);
    priorizedListObject o4 = new priorizedListObject(4, 4, 0, "bollore", 2, new Date (simpledateformat.parse("01/05/2017").getTime()));
    priorizedList.add(o4);
    priorizedListObject o5 = new priorizedListObject(5, 5, 0, "bollore", 8, new Date (simpledateformat.parse("10/06/2017").getTime()));
    priorizedList.add(o5);
   
    return priorizedList;
   
}





}


