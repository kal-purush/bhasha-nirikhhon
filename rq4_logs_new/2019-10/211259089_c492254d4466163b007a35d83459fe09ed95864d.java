import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CustomMap {
  public static Map<Object, Object> map = new HashMap<Object, Object>();
  static {

  }

  public <K, V> void readProperties(File file) {
    Properties props;
    try {
      props = new Properties();
      props.load(getClass().getClassLoader().getResourceAsStream(file.getName()));
    } catch (FileNotFoundException e) {

    } catch (IOException e) {
    }
  }

  public static void writeProperties(File file) {
    try {
      ObjectOutputStream oos = new ObjectOutputStream (new FileOutputStream(file));
      oos.writeObject(map);
      //TODO closing
      oos.close();
    } catch (Exception e) {
      // Catch exceptions
    }
  }

  public static <K, V> void put(K key, V value) {
    map.put(key, value);
  }

  public static <K> Object get(K key) {
    return map.get(key);
  }

  public static <K> void remove(K key) {
    map.remove(key);
  }

  public static <K> Boolean containsKey(K key) {
    return map.containsKey(key);
  }

  public static <V> Boolean containsValue(V value) {
    return map.containsValue(value);
  }

  public static int size() {
    return map.size();
  }

  public static Map<Object, Object> getMap() {
    return map;
  }

  public static void setMap(Map<Object, Object> map) {
    CustomMap.map = map;
  }

}