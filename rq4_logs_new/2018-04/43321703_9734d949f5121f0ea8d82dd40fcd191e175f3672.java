package edu.duke.globallog.sparklogprocessor;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import mikera.vectorz.Vector;

class Stage
{
  Long ipBytes;
  Long cachedBytes;
  Long shuffleBytesRead; 
  Long shuffleBytesWritten;
  Long opBytes;
  Vector vec;

  Stage(Long a, Long b, Long c, Long d, Long e) {
    ipBytes = a;
    cachedBytes = b;
    shuffleBytesRead = c;
    shuffleBytesWritten = d;
    opBytes = e;
    vec = Vector.of(new double[]{
      a.doubleValue(), b.doubleValue(), c.doubleValue(), d.doubleValue(), e.doubleValue()});
  }

  //implementation of stage matching
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Stage)) {
       return false;
    }
    Stage s = (Stage) o;

    // use vector algebra
System.out.println("--Distance found: " + this.vec.toNormal().distance(s.vec.toNormal()));
    if(distance(s) <= 0.5) {
      return true;
    }
/*
    if(0.5*ipBytes <= s.ipBytes && 2*ipBytes >= s.ipBytes &&
      0.5*cachedBytes <= s.cachedBytes && 2*cachedBytes >= s.cachedBytes &&
      0.5*shuffleBytesRead <= s.shuffleBytesRead && 2*shuffleBytesRead >= s.shuffleBytesRead &&
      0.5*shuffleBytesWritten <= s.shuffleBytesWritten && 2*shuffleBytesWritten >= s.shuffleBytesWritten &&
      0.5*opBytes <= s.opBytes && 2*opBytes >= s.opBytes) {
        return true;
    }
*/
    return false;
  }

  // Find vector distance
  public double distance(Stage s) {
    return this.vec.toNormal().distance(s.vec.toNormal());
  }

  @Override
  public int hashCode() {
    return "".hashCode();
  }

  @Override
  public String toString() {
    return vec.toString();
  }
}

class Config
{
  String appId;
  Long maxHeap;
  Long maxCores;
  Long yarnOverhead;
  Long numExecs;
  Double sparkMemoryFraction;
  Boolean offHeap;
  Long offHeapSize;
  String serializer;
  String gcAlgo; 
  Long newRatio;

  Config(String app, Long a, Long b, Long c, Long d, Double e, 
    Boolean f, Long g, String h, String i, Long j) {
    appId = app;
    maxHeap = a;
    maxCores = b;
    yarnOverhead = c;
    numExecs = d;
    sparkMemoryFraction = e;
    offHeap = f;
    offHeapSize = g;
    serializer = h;
    gcAlgo = i;
    newRatio = j;
  }

  @Override
  public boolean equals(Object c) {
    if (!(c instanceof Config)) {
       return false;
    }
    Config o = (Config) c;
    if(appId.equals(o.appId) && maxHeap.equals(o.maxHeap) &&
      maxCores.equals(o.maxCores) &&
      yarnOverhead.equals(o.yarnOverhead) &&
      numExecs.equals(o.numExecs) &&
      sparkMemoryFraction.equals(o.sparkMemoryFraction) &&
      offHeap.equals(o.offHeap) &&
      offHeapSize.equals(o.offHeapSize) &&
      serializer.equals(o.serializer) &&
      gcAlgo.equals(o.gcAlgo) &&
      newRatio.equals(o.newRatio)) {
        return true;
    }
    return false;
  }

  // Hack for an experiment, don't use in general
  public boolean relaxedEquals(Object c) {
    if (!(c instanceof Config)) {
       return false;
    }
    Config o = (Config) c;
    if(maxHeap.equals(o.maxHeap) &&
      maxCores.equals(o.maxCores) &&
      yarnOverhead.equals(o.yarnOverhead) &&
      numExecs.equals(o.numExecs) &&
      sparkMemoryFraction.equals(o.sparkMemoryFraction) &&
      newRatio.equals(o.newRatio)) {
        return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return appId.hashCode();
  }

  @Override
  public String toString() {
    return appId;
  }
}

class Metrics
{
  Long failedExecs;
  Long maxStorage;
  Long maxExecution; 
  Long totalTime;
  Long maxUsedHeap;
  Long minUsageGap;
  Long maxOldGenUsed;
  Long totalGCTime;
  Long totalNumYoungGC;
  Long totalNumOldGC;
  Integer multiples;

  Metrics(Long a, Long b, Long c, Long d, Long e, Long f, Long g, Long h, Long i, Long j) {
    failedExecs = a;
    maxStorage = b;
    maxExecution = c;
    totalTime = d;
    maxUsedHeap = e;
    minUsageGap = f;
    maxOldGenUsed = g;
    totalGCTime = h;
    totalNumYoungGC = i;
    totalNumOldGC = j;
    multiples = 1;
  }

  Metrics add(Metrics m) {
    failedExecs += m.failedExecs;
    maxStorage += m.maxStorage;
    maxExecution += m.maxExecution;
    totalTime += m.totalTime;
    maxUsedHeap += m.maxUsedHeap;
    minUsageGap += m.minUsageGap;
    maxOldGenUsed += m.maxOldGenUsed;
    totalGCTime += m.totalGCTime;
    totalNumYoungGC += m.totalNumYoungGC;
    totalNumOldGC += m.totalNumOldGC;
    multiples += m.multiples;
    return this;
  }

  Metrics avg() {
    return new Metrics(failedExecs/multiples, maxStorage/multiples, 
      maxExecution/multiples, totalTime/multiples, maxUsedHeap/multiples,
      minUsageGap/multiples, maxOldGenUsed/multiples, totalGCTime/multiples,
      totalNumYoungGC/multiples, totalNumOldGC/multiples);
  }

  @Override
  public String toString() {
    return  failedExecs +
      "\t" + maxStorage + "\t" + maxExecution +
      "\t" + totalTime + "\t" + maxUsedHeap +
      "\t" + minUsageGap + "\t" + maxOldGenUsed +
      "\t" + totalGCTime + "\t" + totalNumYoungGC +
      "\t" + totalNumOldGC;
  }
}

class ConfigPlusMetrics {
  Config config;
  Metrics metrics;

  ConfigPlusMetrics(Config c, Metrics m) {
    config = c;
    metrics = m;
  }

  public Config getConfig() {
    return config;
  }

  public Metrics getMetrics() {
    return metrics;
  }
}


/**
 * Class to combine stats from spark event logs and resource monitors and summarize them
 * Writes output to mysql db
 *
 */
public class TestRelM
{

  public static enum Mode { SIMILAR, SINGLE };

  public static void main(String[] args) {
    // connection to database
    Connection conn = null;

    final String DB_URL = "jdbc:mysql://localhost/test";
    final String DB_USER = "root";
    final String DB_PASSWORD = "database";

    final String RELM_TABLE = "RELM_DATA";
    final String TEST_RELM_TABLE = "TEST_RELM";

    Long maxHeap = 2*1024*1024*1024L;
    Long yarnOverhead = 1*1024*1024*1024L;
 //   Config conf = new Config("", maxHeap, 2L, yarnOverhead, 10, 0.6, false, 
 //     maxHeap, "java", "parallel", 2L);

    Mode mode = Mode.SIMILAR;
    String resultsPath = "results-similar";

    if("similar".equals(args[0])) {
      if(!("".equals(args[1]))) {
        resultsPath = args[1];
      }
    }

    if("single".equals(args[0])) {
      mode = Mode.SINGLE;
    }

    Stage[] stages = new Stage[5];
    stages[0] = new Stage(132744302L, 134217728L, 0L, 0L, 0L);
    stages[1] = new Stage(134619472L, 0L, 0L, 0L, 0L);
    stages[2] = new Stage(137818123L, 0L, 0L, 41033290L, 0L);
    stages[3] = new Stage(0L, 0L, 57856938L, 33035663L, 0L);
    stages[4] = new Stage(0L, 0L, 11800L, 0L, 0L);

    // for single mode
    Map<Config, Map<Stage, Metrics>> configMap = new HashMap<Config, Map<Stage, Metrics>>();

    // for similar mode
    Map<Stage, Map<Config, List<ConfigPlusMetrics>>> stageMap = new HashMap<Stage, Map<Config, List<ConfigPlusMetrics>>>();

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

      Statement stmt = conn.createStatement();

      String sql1 = "DROP TABLE IF EXISTS " + TEST_RELM_TABLE;
      stmt.executeUpdate(sql1);

      String sql2 = "CREATE TABLE IF NOT EXISTS " + TEST_RELM_TABLE + " (appId VARCHAR(255), " +
             "maxHeap BIGINT, maxCores BIGINT, yarnOverhead BIGINT, numExecs BIGINT, " + 
             "sparkMemoryFraction DECIMAL(4,2), offHeap BOOLEAN, offHeapSize BIGINT, " +
             "serializer VARCHAR(255), gcAlgo VARCHAR(255), newRatio BIGINT, " +
             "failedExecs BIGINT, maxStorage BIGINT, maxExecution BIGINT, totalTime BIGINT, " +
             "maxUsedHeap BIGINT, minUsageGap BIGINT, maxOldGenUsed BIGINT, " +
             "totalGCTime BIGINT, totalNumYoungGC BIGINT, totalNumOldGC BIGINT, " +
             "PRIMARY KEY(appId))";
      stmt.executeUpdate(sql2);

      String qsql1 = "SELECT appId as one, ipBytes as two, cachedBytes as three, " +
             "shuffleBytesRead as four, shuffleBytesWritten as five, opBytes as six, " +
             "maxHeap, maxCores, yarnOverhead, numExecs, sparkMemoryFraction, " +
             "offHeap, offHeapSize, serializer, gcAlgo, newRatio, failedExecs, " +
             "maxStorage, maxExecution, totalTime, maxUsedHeap, minUsageGap, " +
             "maxOldGenUsed, totalGCTime, totalNumYoungGC, totalNumOldGC FROM " +
             RELM_TABLE;
 
      String isql1 = "INSERT INTO " + TEST_RELM_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

      PreparedStatement qstmt1 = conn.prepareStatement(qsql1);
      PreparedStatement istmt1 = conn.prepareStatement(isql1);
      conn.setAutoCommit(false);

      // Scan stages in training data and build configMap
      ResultSet rs1 = qstmt1.executeQuery();

    if(mode.equals(Mode.SINGLE)) {
      for(Stage stage: stages) {
        rs1.beforeFirst();
        while(rs1.next()) {
          // find matching rows, store configs along with row ids
          Stage candidate = new Stage(rs1.getLong("two"), rs1.getLong("three"), 
            rs1.getLong("four"), rs1.getLong("five"), rs1.getLong("six"));
          if(stage.equals(candidate)) {
            Config conf = new Config(rs1.getString("one"), rs1.getLong("maxHeap"), 
              rs1.getLong("maxCores"), rs1.getLong("yarnOverhead"), rs1.getLong("numExecs"), 
              rs1.getDouble("sparkMemoryFraction"),
              rs1.getBoolean("offHeap"), rs1.getLong("offHeapSize"),
              rs1.getString("serializer"), rs1.getString("gcAlgo"), rs1.getLong("newRatio"));
            Metrics met = new Metrics(rs1.getLong("failedExecs"), rs1.getLong("maxStorage"), 
              rs1.getLong("maxExecution"), rs1.getLong("totalTime"), rs1.getLong("maxUsedHeap"),
              rs1.getLong("minUsageGap"), rs1.getLong("totalGCTime"), rs1.getLong("maxOldGenUsed"),
              rs1.getLong("totalNumYoungGC"), rs1.getLong("totalNumOldGC"));

            if(configMap.containsKey(conf)) {
              Map<Stage, Metrics> metricsMap = configMap.get(conf);
              if(metricsMap.containsKey(stage)) {
                metricsMap.get(stage).add(met);
              } else {
                metricsMap.put(stage, met);
              }
            } else {
              Map<Stage, Metrics> metricsMap = new HashMap<Stage, Metrics>();
              metricsMap.put(stage, met);
              configMap.put(conf, metricsMap);
            }
          }
        }
      }

      // parse configMap and write to test table
      for(Config conf: configMap.keySet()) {
        Map<Stage, Metrics> metricsMap = configMap.get(conf);
        if(metricsMap == null || metricsMap.isEmpty() || (metricsMap.size() % stages.length) != 0) {
          continue;
        }
        
        istmt1.clearParameters();
        istmt1.setObject(1, conf.appId);
        istmt1.setObject(2, conf.maxHeap);
        istmt1.setObject(3, conf.maxCores);
        istmt1.setObject(4, conf.yarnOverhead);
        istmt1.setObject(5, conf.numExecs);
        istmt1.setObject(6, conf.sparkMemoryFraction);
        istmt1.setObject(7, conf.offHeap);
        istmt1.setObject(8, conf.offHeapSize);
        istmt1.setObject(9, conf.serializer);
        istmt1.setObject(10, conf.gcAlgo);
        istmt1.setObject(11, conf.newRatio);

        Long failedExecs = 0L;
        Long maxStorage = 0L;
        Long maxExecution = 0L;
        Long totalTime = 0L;
        Long maxUsedHeap = 0L;
        Long minUsageGap = Long.MAX_VALUE;
        Long maxOldGenUsed = 0L;
        Long totalGCTime = 0L;
        Long totalNumYoungGC = 0L;
        Long totalNumOldGC = 0L;

        for(Stage stage: metricsMap.keySet()) {
          Metrics met = metricsMap.get(stage).avg();
          failedExecs += met.failedExecs;
          maxStorage = Math.max(maxStorage, met.maxStorage);
          maxExecution = Math.max(maxExecution, met.maxExecution);
          totalTime += met.totalTime;
          maxUsedHeap = Math.max(maxUsedHeap, met.maxUsedHeap);
          minUsageGap = Math.min(minUsageGap, met.minUsageGap);
          maxOldGenUsed = Math.max(maxOldGenUsed, met.maxOldGenUsed);
          totalGCTime += met.totalGCTime;
          totalNumYoungGC += met.totalNumYoungGC;
          totalNumOldGC += met.totalNumOldGC;
        }

        istmt1.setObject(12, failedExecs);
        istmt1.setObject(13, maxStorage);
        istmt1.setObject(14, maxExecution);
        istmt1.setObject(15, totalTime);
        istmt1.setObject(16, maxUsedHeap);
        istmt1.setObject(17, minUsageGap);
        istmt1.setObject(18, maxOldGenUsed);
        istmt1.setObject(19, totalGCTime);
        istmt1.setObject(20, totalNumYoungGC);
        istmt1.setObject(21, totalNumOldGC);
        istmt1.addBatch();
      }

System.out.println("--Running " + istmt1);
      istmt1.executeBatch();
      conn.commit();
    } else {
      // SIMILAR mode starts
      while(rs1.next()) {
        // find matching rows, store configs along with row ids
        Stage candidate = new Stage(rs1.getLong("two"), rs1.getLong("three"),
             rs1.getLong("four"), rs1.getLong("five"), rs1.getLong("six"));
        Config conf = new Config(rs1.getString("one"), rs1.getLong("maxHeap"),
              rs1.getLong("maxCores"), rs1.getLong("yarnOverhead"), rs1.getLong("numExecs"),
              rs1.getDouble("sparkMemoryFraction"),
              rs1.getBoolean("offHeap"), rs1.getLong("offHeapSize"),
              rs1.getString("serializer"), rs1.getString("gcAlgo"), rs1.getLong("newRatio"));
        Config confExceptApp = new Config("", rs1.getLong("maxHeap"),
              rs1.getLong("maxCores"), rs1.getLong("yarnOverhead"), rs1.getLong("numExecs"),
              rs1.getDouble("sparkMemoryFraction"),
              rs1.getBoolean("offHeap"), rs1.getLong("offHeapSize"),
              rs1.getString("serializer"), rs1.getString("gcAlgo"), rs1.getLong("newRatio"));
        Metrics met = new Metrics(rs1.getLong("failedExecs"), rs1.getLong("maxStorage"),
              rs1.getLong("maxExecution"), rs1.getLong("totalTime"), rs1.getLong("maxUsedHeap"),
              rs1.getLong("minUsageGap"), rs1.getLong("totalGCTime"), rs1.getLong("maxOldGenUsed"),
              rs1.getLong("totalNumYoungGC"), rs1.getLong("totalNumOldGC"));

        if(stageMap.containsKey(candidate)) {
          // check for existing config
          Map<Config, List<ConfigPlusMetrics>> confMap = stageMap.get(candidate);
          if(confMap.containsKey(confExceptApp)) {
            List<ConfigPlusMetrics> confList = confMap.get(confExceptApp);
            confList.add(new ConfigPlusMetrics(conf, met));
            confMap.put(confExceptApp, confList);
          } else {
            // new entry in confMap
            List<ConfigPlusMetrics> confList =
                new ArrayList<ConfigPlusMetrics>();
            confList.add(new ConfigPlusMetrics(conf, met));
            confMap.put(confExceptApp, confList);
          }
        } else {
          // new entry in map
          Map<Config, List<ConfigPlusMetrics>> confMap = 
              new HashMap<Config, List<ConfigPlusMetrics>>();
          List<ConfigPlusMetrics> confList = 
              new ArrayList<ConfigPlusMetrics>();
          confList.add(new ConfigPlusMetrics(conf, met));
          confMap.put(confExceptApp, confList);
          stageMap.put(candidate, confMap);
        }
      }

      // print map to csvs
      int cnt = 1;
      for(Stage stage: stageMap.keySet()) {
        BufferedWriter writer = new BufferedWriter(new FileWriter(
          new File(resultsPath + "/" + cnt + ".tsv")));
        writer.write(stage + System.getProperty("line.separator"));

System.out.println("*Stats for stage: " + stage);
        Map<Config, List<ConfigPlusMetrics>> confMap = stageMap.get(stage);
        for(Config conf: confMap.keySet()) {
//System.out.println("**Conf map found: " + conf);
          List<ConfigPlusMetrics> confList = confMap.get(conf);
          for(ConfigPlusMetrics confMet: confList) {
//System.out.println("***Metrics for conf " + confMet.getConfig() + ": " + confMet.getMetrics());
            writer.write(confMet.getConfig() + "\t" + confMet.getMetrics() +
              System.getProperty("line.separator"));
          }
        }
        writer.close();
        cnt++;
      }

    }

      try { qstmt1.close(); } catch(Exception e) {}
      try { istmt1.close(); } catch(Exception e) {}
    } 
    catch(Exception e) {
          e.printStackTrace();
    } finally {
          try { conn.close(); } catch(Exception e) {}
    }
  }
}