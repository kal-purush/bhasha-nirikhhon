package uci.ics.mondego.tldr.changeanalyzer;

import java.io.IOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import uci.ics.mondego.tldr.indexer.RedisHandler;

public abstract class ChangeAnalyzer {

	protected static final Logger logger = LogManager.getLogger(ClassChangeAnalyzer.class);
	private final String entityName;
	private boolean changed;
	private boolean isSynced;
	protected RedisHandler rh;
	
	public ChangeAnalyzer(String className){
		this.entityName = className;
		this.changed = false;
		this.isSynced = false;
		this.rh = RedisHandler.getInstane();
	}
	
	public boolean hasChanged(){
		return changed;
	}
	
	public String getEntityName(){
		return entityName;
	}
	
	protected void setChanged(boolean bool) {
		this.changed = bool;
	}
	
	public boolean hasSynced(){
		return isSynced;
	}
	
	protected void sync(String name, String newCheckSum){
		rh.update(name, newCheckSum);
		this.isSynced = true;
	}
	
	protected abstract void parse() throws IOException;
}