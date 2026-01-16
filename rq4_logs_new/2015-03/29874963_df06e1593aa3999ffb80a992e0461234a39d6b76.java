

import java.io.Serializable;
import java.util.ArrayList;
import java.lang.StringBuffer;


public class PageRankEntry implements Comparable<PageRankEntry>, Serializable{
	
	
	public int docID;
	public String docName;
	public double pageRank;
	

	public PageRankEntry(int docID, String name, double pageRank){
	
		this.docID=docID;
		this.docName=name;
		this.pageRank=pageRank;
		
	}
	
	public int compareTo( PageRankEntry other){
	
		return Double.compare(other.pageRank,this.pageRank);
		
	}
	
	
	public String toString(){
		
		StringBuffer buf=new StringBuffer();
		
		buf.append(docID);
		buf.append(" ");
		buf.append(docName);
		buf.append(" ");
		buf.append(pageRank);
		
		return buf.toString();
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}