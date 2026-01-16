/*
 * To change this license header, choose License Headers in Project Properties. To change this
 * template file, choose Tools | Templates and open the template in the editor.
 */
package com.chatbot.permit.municipal.zones;

import java.awt.Polygon;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import javax.swing.JFileChooser;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;



/**
 *
 * @author bmurray
 */
public class MapHandler {
  private final Connection myConn;
  private static List<ZonePolygon> mapZones;



  public MapHandler() throws Exception {
    String dbName = "TEST";
    String dbUser = "test";
    String dbPwd = "test";
    String dbURL = "jdbc:derby://localhost:1527/kmlFile";

    myConn = DriverManager.getConnection(dbURL, dbUser, dbPwd);
    // this.parseKML(); //uncomment this to import file on start
    // System.out.println("DB connection successful to: " + dbURL);
    mapZones = this.convertAllZonesToPolygon();

  }

  /**
   * This function returns a list of the custom ZonePolygon objects that are in the DB.
   * 
   * @return
   */
  public List<ZonePolygon> getMapZones() {
    return mapZones;
  }

  /**
   * This function parses through a KML file and imports the contents into two tables. One table
   * stores the the ID, the zone, and the zone again as a note. The second table stores the ID, the
   * lat and lon, and the lat and lon converted to ints.
   * 
   * @throws Exception
   */
  public void parseKML() throws Exception {
    try {

      final JFileChooser fc = new JFileChooser();
      // Open the dialog using null as parent component if you are outside a
      // Java Swing application otherwise provide the parent comment instead
      int returnVal = fc.showOpenDialog(null);
      if (returnVal == JFileChooser.APPROVE_OPTION) {
        // Retrieve the selected file
        File file = fc.getSelectedFile();
        try (FileInputStream inputFile = new FileInputStream(file)) {
          // Do something here

          DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
          DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
          Document doc = dBuilder.parse(inputFile);
          doc.getDocumentElement().normalize();
          NodeList nList = doc.getElementsByTagName("Placemark");

          for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);

            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
              Element eElement = (Element) nNode;

              // Store ZoneID
              String ZoneName = eElement.getElementsByTagName("name").item(0).getTextContent();
              addZone(temp, ZoneName);

              // Scan input for Cordniates in zone and store in table
              Scanner input = new Scanner(
                  eElement.getElementsByTagName("coordinates").item(0).getTextContent());
              while (input.hasNextLine()) {
                String lineIn = input.nextLine().trim();
                if (lineIn.contains(",")) {
                  int first = lineIn.indexOf(',');
                  int second = lineIn.indexOf(',', (first + 1));
                  String tempLat = lineIn.substring(0, first);
                  String tempLon = lineIn.substring((first + 1), second);
                  String tempz = lineIn.substring((second + 1));
                  addZoneCord(temp, tempLat, tempLon);
                }
              }
            }
          }
        }
      }

      // File inputFile = new File("input.txt");

    } catch (Exception e) {
      e.printStackTrace();
    }


  }

  /**
   * This function takes in coordinates and then loops through all polygons in the db until it finds
   * one that contains the coordinates.
   * 
   * @param lat
   * @param lon
   * @return Returns the zone id if found or a -1 if not found.
   * @throws Exception
   */
  public int findZones(Double lat, Double lon) throws Exception {
    int foundZone;
    int numZones = mapZones.size();
    int tempLat = (int) (lat * 1000000);
    int tempLon = (int) (lon * 1000000);

    try {
      // Loop through all zones to look for polygon that contains the cordinates.
      int index1 = 0;
      while (index1 < numZones) {
        // If true, return zone id found
        if (mapZones.get(index1).contains(tempLat, tempLon)) {
          foundZone = mapZones.get(index1).getZoneID();
          return foundZone;
        }
        index1++;
      }
    } catch (Exception exc) {
      System.out.println(exc.getMessage());

    }
    return -1;// returns negative one if no zone is found.
  }

  /**
   * This function takes in the variables need to add the a zone to a database.
   * 
   * @param uniq_Zone_id
   * @param zone
   * @throws Exception
   */
  public void addZone(int uniq_Zone_id, String zone) throws Exception {
    Statement myStmt = null;
    ResultSet myRs = null;
    try {
      myStmt = myConn.createStatement();
      String sql1 = "INSERT INTO polygons (POLYGON_ID, ZONE_CODE, ZONE_NOTE) " + "VALUES("
          + uniq_Zone_id + ", '" + zone + "', '" + zone + "')";
      myStmt.executeUpdate(sql1);
    } finally {
      close(myStmt, myRs);
    }
  }

  /**
   * This function takes in the variables need to add the coordinates to a database.
   * 
   * @param uniq_Zone_id
   * @param lat
   * @param lon
   * @throws Exception
   */
  public void addZoneCord(int uniq_Zone_id, String lat, String lon) throws Exception {
    Statement myStmt = null;
    ResultSet myRs = null;
    try {
      myStmt = myConn.createStatement();
      Double templat = Double.parseDouble(lat);
      Double templon = Double.parseDouble(lon);
      int templatCord = (int) (templat * 1000000);
      int templonCord = (int) (templon * 1000000);
      String sql1 = "INSERT INTO maps (FK_POLYGON_ID, LAT, LON, LAT_CORD, LON_CORD) " + "VALUES("
          + uniq_Zone_id + ", " + templat + ", " + templon + ", " + templatCord + ", " + templonCord
          + ")";

      myStmt.executeUpdate(sql1);

    } finally {
      close(myStmt, myRs);
    }
  }

  /**
   * This function converts a single row in the DB into Polygons and adds them to a list.
   * 
   * @param zoneID
   * @return Custom class ZonePolygon object.
   * @throws SQLException
   */
  public ZonePolygon convertZoneToPolygon(int zoneID) throws SQLException {

    ZonePolygon tempZone = new ZonePolygon();
    Statement myStmt = null;
    ResultSet myRsCount = null;
    ResultSet myRs = null;

    try {
      myStmt = myConn.createStatement();
      myRs = myStmt.executeQuery("SELECT * FROM maps WHERE FK_POLYGON_ID = " + zoneID);

      if (myRs != null) {
        tempZone.setZoneID(zoneID);

        while (myRs.next()) {
          int tempLat = Integer.parseInt(myRs.getString("LAT_CORD"));
          int tempLon = Integer.parseInt(myRs.getString("LON_CORD"));
          tempZone.addPoint(tempLat, tempLon);

        }
      }
      return tempZone;
    } finally {
      close(myStmt, myRs);
    }


  }

  /**
   * This function converts all zones in the DB into Polygons and adds them to a list.
   * 
   * @return List of custom class ZonePolygons.
   * @throws SQLException
   */
  public List<ZonePolygon> convertAllZonesToPolygon() throws SQLException {

    List<ZonePolygon> list = new ArrayList<>();
    Statement myStmt = null;
    ResultSet myRs1 = null;
    try {
      myStmt = myConn.createStatement();
      myRs1 = myStmt.executeQuery("SELECT DISTINCT FK_POLYGON_ID FROM MAPS");

      if (myRs1 != null) {
        while (myRs1.next()) {
          // todo

          ZonePolygon tempZone =
              convertZoneToPolygon(Integer.parseInt(myRs1.getString("FK_POLYGON_ID")));
          list.add(tempZone);


        }
      }
      return list;
    } finally {
      close(myStmt, myRs1);
    }
  }

  /**
   * This function returns the string value of the zone code
   * 
   * @param zoneID
   * @return string value of zone code
   * @throws SQLException
   */
  public String getZoneCodeForID(int zoneID) throws SQLException {
    Statement myStmt = null;
    ResultSet myRs = null;
    String zoneCode = null;
    try {
      myStmt = myConn.createStatement();
      myRs = myStmt.executeQuery("SELECT * FROM polygons WHERE POLYGON_ID = " + zoneID);
      if (myRs != null) {
        while (myRs.next()) {
          zoneCode = myRs.getString("ZONE_CODE");
        }
      }
      return zoneCode;
    } finally {
      close(myStmt, myRs);
    }
  }

  /**
   * This function returns the string value of any 2nd zone code associated.
   * 
   * @param zoneID
   * @return string value of zone code Will return NULL if no value found
   * @throws SQLException
   */
  public String getZoneCode2ForID(int zoneID) throws SQLException {
    Statement myStmt = null;
    ResultSet myRs = null;
    String zoneCode = null;
    try {
      myStmt = myConn.createStatement();
      myRs = myStmt.executeQuery("SELECT * FROM polygons WHERE POLYGON_ID = " + zoneID);
      if (myRs != null) {
        while (myRs.next()) {
          zoneCode = myRs.getString("ZONE_CODE_2");
        }
      }
      return zoneCode;
    } finally {
      close(myStmt, myRs);
    }
  }

  /**
   * This function returns the string value of any 3rd zone code associated.
   * 
   * @param zoneID
   * @return string value of zone code Will return NULL if no value found
   * @throws SQLException
   */
  public String getZoneCode3ForID(int zoneID) throws SQLException {
    Statement myStmt = null;
    ResultSet myRs = null;
    String zoneCode = null;
    try {
      myStmt = myConn.createStatement();
      myRs = myStmt.executeQuery("SELECT * FROM polygons WHERE POLYGON_ID = " + zoneID);
      if (myRs != null) {
        while (myRs.next()) {
          zoneCode = myRs.getString("ZONE_CODE_3");
        }
      }
      return zoneCode;
    } finally {
      close(myStmt, myRs);
    }
  }

  /**
   * This function returns the string value of any note tied to the zone code associated.
   * 
   * @param zoneID
   * @return string value of zone note Will return NULL if no value found
   * @throws SQLException
   */
  public String getZoneNoteForID(int zoneID) throws SQLException {
    Statement myStmt = null;
    ResultSet myRs = null;
    String zoneCode = null;
    try {
      myStmt = myConn.createStatement();
      myRs = myStmt.executeQuery("SELECT * FROM polygons WHERE POLYGON_ID = " + zoneID);
      if (myRs != null) {
        while (myRs.next()) {
          zoneCode = myRs.getString("ZONE_NOTE");
        }
      }
      return zoneCode;
    } finally {
      close(myStmt, myRs);
    }
  }



  private static void close(Connection myConn, Statement myStmt, ResultSet myRs)
      throws SQLException {

    if (myRs != null) {
      myRs.close();
    }
    if (myStmt != null) {
    }
    if (myConn != null) {
      myConn.close();
    }
  }

  private void close(Statement myStmt, ResultSet myRs) throws SQLException {
    close(null, myStmt, myRs);
  }


}

