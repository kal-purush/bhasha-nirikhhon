package _cache.driver;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;


public class LRU {
private final static  int SIZE=5;
private static int count=0;
public static List<LRUEntry> csvlist = new ArrayList<LRUEntry>();

public static int getCount() {
	return count;
}

public static int getSize() {
	return SIZE;
}

public static void insertToCache(String domain,String IP) {
	DateTimeFormatter dtf3 = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"); 	
	String TS= dtf3.format(LocalDateTime.now()).toString();					
	LRUEntry data = new LRUEntry();
	data.setDomain(domain);
	data.setTimestamp(TS);
	data.setIPAddress(IP);	
	if(count<SIZE) {
	
		csvlist.add(data);	
		++count;
	}
	else if(count==SIZE){
		System.out.println("Cache is Full....!!! Unable to Add to the Cache");
		System.out.println("Implement LRU Cache Replacement Algorithm.......");
		deleteLRUEntry();	
		csvlist.add(data);
	}


}

public static void deleteLRUEntry() {
	//sort csvlist by timestamps..
		//delete first entry
	System.out.println("Deleting Least Recently Used Cache Entry");
	Collections.sort(csvlist);
	csvlist.remove(0);
	--count;

}


public static void update(String searchedURL) {	
	DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
	for(LRUEntry r:csvlist) {
		//forloop and search the entry which has the searched domain..
		// update the time stamp value.
		if(r.getDomain().equals(searchedURL)) {
			r.setTimestamp(dtf2.format(LocalDateTime.now()).toString());
		}
	}
	

}

public static String checkCache(String searchedURL) throws InterruptedException {
	
	for(LRUEntry r:csvlist) {
		//forloop and search the entry which has the searched domain..
		// update the time stamp value.
		if(r.getDomain().equals(searchedURL)) {
			Thread.sleep(3000);
			update(searchedURL);
			return r.getIPAddress();			
		}

	}
	return null;
	
}

 
public static void printCacheEntries() {
	System.out.println("check 2");
	for(LRUEntry q:csvlist) {
	
		System.out.println(q.getTimestamp());
		System.out.println(q.getDomain());
		System.out.println(q.getIPAddress());
	}
}


//public static void main(String[] args) throws InterruptedException {
//	
//	System.out.print("LRU program\n");
//	insertToCache("fb.com","192.168.56.101");
//	Thread.sleep(5000);
//	insertToCache("fb2.com","192.168.56.102");
//	Thread.sleep(3000);
//	insertToCache("fb3.com","192.168.56.103");
//	Thread.sleep(6000);
//	insertToCache("fb4.com","192.168.56.104");
//
//Thread.sleep(6000);
//insertToCache("fb5.com","192.168.56.105");
//
//printCacheEntries();
//Thread.sleep(2000);
//insertToCache("fb6.com","192.168.56.106");
//printCacheEntries();
//
//System.out.println("The ip address is    .........   "+checkCache("fb3.com"));
//printCacheEntries();
//
//}

}
