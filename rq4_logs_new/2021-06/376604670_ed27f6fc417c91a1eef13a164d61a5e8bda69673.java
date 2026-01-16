package cache;

import lombok.Data;
import model.fildClass.*;

import java.util.HashMap;
import java.util.Map;
@Data
public class Cache {

    private Map<String, HashMap<String,?>> cacheDate;

    public Cache() {
        this.cacheDate = new HashMap<>();
        this.cacheDate.put("ApplicationName",new HashMap<String,ApplicationName>());
        this.cacheDate.put("ComputerName",new HashMap<String,ComputerName>());
        this.cacheDate.put("ContextOneC",new HashMap<String,ContextOneC>());
        this.cacheDate.put("ProcessName", new HashMap<String,ProcessName>());
        this.cacheDate.put("Sql",new HashMap<String,Sql>());
        this.cacheDate.put("UserEvent",new HashMap<String,UserEvent>());
    }

}