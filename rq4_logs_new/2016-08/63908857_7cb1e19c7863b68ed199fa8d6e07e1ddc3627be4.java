package xdb;

import com.wiredtiger.db.*;
import java.nio.*;
import java.io.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeSeriesDB3 {
  private static Logger log = LogManager.getLogger(TimeSeriesDB3.class);

  public static class Tuple2<T1 extends Comparable, T2 extends Comparable> implements Comparable<Tuple2> {

    public final T1 v1;
    public final T2 v2;

    public T1 v1() {
      return v1;
    }

    public T2 v2() {
      return v2;
    }

    public int compareTo(Tuple2 t) {
      int c = v1.compareTo(t.v1);
      return c !=0 ? c : v2.compareTo(t.v2);
    }

    public Tuple2(T1 v1, T2 v2) {
      this.v1 = v1;
      this.v2 = v2;
    }

    public int hashCode() {
      return v1.hashCode() | v2.hashCode();
    }

    public boolean equals(Object o) {
      if(o instanceof Tuple2) {
        Tuple2 t = (Tuple2)o;
        return v1.equals(t.v1) && v2.equals(t.v2);
      }
      return false;
    }

    public String toString() {
      return v1.toString()+"#"+v2.toString();
    }
  }

  public interface Tuple {
    static <T1 extends Comparable, T2 extends Comparable> Tuple2<T1, T2> tuple(T1 v1, T2 v2) {
      return new Tuple2<>(v1, v2);
    }
  }

  public static class Event {
    public String host;
    public String metric;
    public long ts;
    public byte[] val;

    public Event(String host, String metric, long ts, byte[] val) {
      this.host = host;
      this.metric = metric;
      this.ts = ts;
      this.val = val;
    }

    public Event(long ts, String host, String metric) {
      this(host, metric, ts, null);
    }

    public String getHost() {
      return host;
    }

    public String getMetric() {
      return metric;
    }

    public String toString() {
      return host +"#"+ metric + "#"+ts;
    }
  }

  public static class EventFactory {
    private Random rnd;
    private String[] metrics;
    private String[] hosts;

    public EventFactory(int numhosts, int nummetrics) {
      hosts = new String[numhosts];
      metrics = new String[nummetrics];
      for(int i = 0; i < numhosts; i++) {
        hosts[i] = "host"+i;
      }
      for(int i = 0; i < nummetrics; i++) {
        metrics[i] = "metric"+i;
      }
      rnd = new Random(numhosts+nummetrics);
    }

    public Event getNextEvent(int salt) {
      return new Event(hosts[rnd.nextInt(hosts.length)], metrics[rnd.nextInt(metrics.length)], Instant.now().toEpochMilli()+salt, new byte[64]);
    }

    public static Event getStartEvent() {
      return new Event("zzzz", "zzzz", 0, new byte[64]);
    }

    public static Event getEndEvent(long ts) {
      return new Event("zzzz", "zzzz", ts, new byte[64]);
    }

  }

  public static boolean checkDir(String dir) {
    boolean ret = true;
    File d = new File(dir);
    if(d.exists()) {
      if(d.isFile())
        ret = false;
    } else {
      d.mkdirs();
    }
    return ret;
  }

  public static class Ingestor implements Runnable {
    int count;
    public Ingestor(int count) {
      this.count = count;
    }

    public void run() {
      int id = (int)Thread.currentThread().getId();
      log.info("ingestor starts {}", id);
      Session session = conn.open_session(null);
      session.create(table, storage);
      int batch = 1000;
      int total = 0;
      EventFactory producer = new EventFactory(10000000, 100000);
      Cursor c = session.open_cursor(table, null, null);
      while(!stop) {
        try {Thread.currentThread().sleep(id);} catch(Exception ex) {}
        boolean done = false;
        try {
          //session.begin_transaction(tnx);
          for(int i = 0; i < batch; i++) {
            Event evt = producer.getNextEvent(id);
            c.putKeyLong(evt.ts);
            c.putKeyString(evt.host);
            c.putKeyString(evt.metric);
            c.putValueByteArray(evt.val);
            c.insert();
          }
          done = true;
          if(batch >= count)
            break;
        } catch(WiredTigerRollbackException e) {
          //session.rollback_transaction(tnx);
          log.info("ingestor roll back");
        } finally {
          if(done) {
            //session.commit_transaction(null);
            counter.addAndGet(batch);
            total += batch;
          }
        }
        if(total%1000000 == 0) {
          log.info("total {}", total);
        }
      }
      c.close();
      session.close(null);
      log.info("ingestor ends {}", counter.get());
    }

  }

  public static class TTLMonitor implements Runnable {
    Session session;

    public TTLMonitor() {
      this.session = conn.open_session(null);
      this.session.create(table, storage);
    }

    public void run() {
      int interval = 3;
      while(!stop) {
        try {Thread.currentThread().sleep(interval*1000);} catch(Exception ex) {}
        Cursor start = null;
        Cursor stop = null;
        boolean done = false;
        try {
          session.snapshot("name=past");
          //session.begin_transaction(tnx);
          Event evt1 = EventFactory.getStartEvent();
          Cursor mc = session.open_cursor(table, null, null);
          mc.putKeyLong(evt1.ts);
          mc.putKeyString(evt1.host);
          mc.putKeyString(evt1.metric);
          mc.putValueByteArray(evt1.val);
          mc.insert();
          Event evt2 = EventFactory.getEndEvent(past(interval*10));
          mc.putKeyLong(evt2.ts);
          mc.putKeyString(evt2.host);
          mc.putKeyString(evt2.metric);
          mc.putValueByteArray(evt2.val);
          mc.insert();
          mc.close();
          start = session.open_cursor(table, null, null);
          start.putKeyLong(evt1.ts);
          start.putKeyString(evt1.host);
          start.putKeyString(evt1.metric);
          start.putValueByteArray(evt1.val);
          int r1 = start.search();
          stop = session.open_cursor(table, null, null);
          stop.putKeyLong(evt2.ts);
          stop.putKeyString(evt2.host);
          stop.putKeyString(evt2.metric);
          stop.putValueByteArray(evt2.val);
          int r2 = stop.search();
          if(r1==0 && r2==0) {
            int nd = 0;
            log.info("start delete");
            while(start.next()==0) {
              long ts = start.getKeyLong();
              String host = start.getKeyString();
              String metric = start.getKeyString();
              if (ts == evt2.ts && host == evt2.host && metric == evt2.metric)
                break;
              start.putKeyLong(ts);
              start.putKeyString(host);
              start.putKeyString(metric);
              start.remove();
              nd++;
            }
            log.info("delete {}", nd);
          }
          done = true;
          /*if(r1==0 && r2==0) {
            int ret = session.truncate(null, start, stop, null);
            log.info("truncate ret {} for the past {} seconds ", ret, (stop.getKeyLong() - start.getKeyLong())/1000);
          } else {
            log.info("not found");
            }*/
        } catch(WiredTigerRollbackException e) {
          log.info("ttl monitor roll back");
        } finally {
          if(start != null)
            start.close();
          if(stop != null)
            stop.close();
          if(done) {
            //session.commit_transaction(null);
          } else {
            //session.rollback_transaction(tnx);
          }
          session.snapshot("drop=(all)");
        }
      }
      ttlstopped = true;
      log.info("TTL scanning ends ");
    }
  }

  static long past(int seconds) {
    return Instant.now().toEpochMilli() - seconds*1000;
  }

  public static class Analyst implements Runnable {

    private Random rnd;
    private int interval;

    public Analyst() {
      rnd = new Random();
      interval = 10;
    }

    private void report(List<Event> evts) {
      log.info("For the past {} seconds, select count(*) from events group by host, metric rows = {}", interval,
               evts
               .stream()
               .collect((
                         Collectors.groupingBy(
                                               evt -> Tuple.tuple(evt.host, evt.metric),
                                               Collectors.counting())))
               .entrySet().stream()
               .sorted(Comparator.comparing(Map.Entry::getKey))
               .collect(Collectors.toList())
               .size()
               );
    }

    public void run() {
      int ret;
      Session session = conn.open_session(null);
      session.create(table, storage);
      while(!stop) {
        try {Thread.currentThread().sleep(rnd.nextInt(2000));} catch(Exception ex) {}
        Cursor c = null;
        List<Event> evts = new ArrayList<Event>();
        try {
          session.snapshot("name=past");
          c = session.open_cursor(table, null, null);
          long ts = past(10);
          c.putKeyLong(ts);
          SearchStatus status = c.search_near();
          switch(status) {
          case NOTFOUND:
            //log.info("no data points");
            break;
          case SMALLER:
            //log.info("beyond time horizon {}", (ts-c.getKeyLong())/1000.0);
            break;
          case FOUND:
          case LARGER:
            do {
              evts.add(new Event(c.getKeyLong(), c.getKeyString(), c.getKeyString()));
            } while(c.next() == 0);
            break;
          }
        } catch(WiredTigerRollbackException e) {
          log.info("analyst roll back");
        } catch(WiredTigerException e) {
          log.info("analyst {}", e);
        } finally {
          if(c != null)
            c.close();
          session.snapshot("drop=(all)");
        }
        if(evts.size() > 0)
          report(evts);
      }
      session.close(null);
    }

  }

  private static volatile boolean stop = false;
  private static boolean ttlstopped = false;

  private static final String db = "./tsdb";
  private static final String table = "table:metrics";
  private static final String cols = "columns=(ts,host,metric,val)";
  private static final String storage = "type=lsm,key_format=qSS,value_format=u,"+cols;
  private static final String tnx = "isolation=snapshot";
  private static final int ttl = 3000;
  private static final String ttlindex = "index:metrics:ttl";
  private static final String ttlcol = "columns=(ttl)";
  private static final String tsindex = "index:metrics:ts";
  private static final String tscol = "columns=(ts)";

  static AtomicInteger counter = new AtomicInteger(0);

  private static void init() {
    checkDir(db);
    conn = wiredtiger.open(db, "create,statistics=(all),statistics_log=(wait=10)");
  }

  private static Connection conn;

  public static void main( String[] args ) throws Exception {
    init();
    int count = 2000000000;
    int pn = 8;
    int rn = 0;

    for (int i= 0; i < pn; i++) {
      new Thread(new Ingestor(count/pn)).start();
    }

    for (int i= 0; i < rn; i++) {
      new Thread(new Analyst()).start();
    }

    //new Thread(new TTLMonitor()).start();

    while(true) {
      int c1= counter.get();
      try {Thread.currentThread().sleep(1000);} catch(Exception ex) {}
      int c2 = counter.get();
      if(c2 >= count) {
        stop = true;
        break;
      }
      log.info("evts processed {} {}/{}", c2-c1, c2, count);
    }
    log.info("counter={}", counter.get());
    Session session = conn.open_session(null);
    session.create(table, storage);
    try {Thread.currentThread().sleep(10000);} catch(Exception ex) {}
    log.info("THE END");
    session.compact(table, null);
    session.drop(table, null);
    conn.close(null);
  }

}