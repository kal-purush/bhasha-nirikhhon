// WeatherBean.java
// WeatherBean maintains weather information for one city.

import javax.swing.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.Properties;

public class WeatherBean implements Serializable
{
   private String cityName;
   private ImageIcon imageToday;
   private ImageIcon imageTomorrow;

   private WeatherParam yesterday;
   private WeatherParam today;
   private WeatherParam tomorrow;

   private static Properties imageNames; 
   
   // initialize imageNames when class WeatherInfo is loaded into memory
   static
   {
      imageNames = new Properties(); // create properties table
      
      // load weather descriptions and image names from properties file
      try
      {
         // load properties file contents
         imageNames.load( new FileInputStream( "imagenames.properties" ) );
      }
      catch ( IOException ioException )
      {
         System.out.println("FILE EXCEPTION");
         ioException.printStackTrace();
      }
   }

   public WeatherBean(String city, WeatherParam yesterday, WeatherParam today, WeatherParam tomorrow)
   {
      this.cityName=city;
      this.yesterday=yesterday;
      this.today=today;
      this.tomorrow=tomorrow;

      URL url = WeatherBean.class.getResource( "images/" + imageNames.getProperty( today.getWeatherForecast(), "noinfo.jpg" ) );

      // get weather image name or noinfo.jpg if weather description not found
      imageToday = new ImageIcon( url );

      url = WeatherBean.class.getResource( "images/" + imageNames.getProperty( tomorrow.getWeatherForecast(), "noinfo.jpg" ) );

      // get weather image name or noinfo.jpg if weather description not found
      imageTomorrow = new ImageIcon( url );
   }

   public String getCityName()
   { 
      return cityName; 
   }

   public ImageIcon getImageToday() {
      return imageToday;
   }

   public ImageIcon getImageTomorrow() {
      return imageTomorrow;
   }

   public WeatherParam getYesterday() {
      return yesterday;
   }

   public WeatherParam getToday() {
      return today;
   }

   public WeatherParam getTomorrow() {
      return tomorrow;
   }
}