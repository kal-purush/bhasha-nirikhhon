/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   First version:  Johan Boye, 2010
 *   Second version: Johan Boye, 2012
 *   Additions: Hedvig Kjellström, 2012-14
 */  


package ir;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.ArrayList;
import java.lang.Math;
import java.io.*;



/**
 *   Implements an inverted index as a Hashtable from Biwords to PostingsLists.
 */
public class BiwordIndex implements Index {

    /** The index as a hashtable. */
    private HashMap<String,PostingsList> index = new HashMap<String,PostingsList>(10000);

    private String lastToken=null;
    private int lastTokenOffset=-1;
    private int lastDocID=-1;
    
    

    public BiwordIndex(){
    	    
    }
    

    /**
     *  Inserts this token in the index.
     */
    public void insert( String token, int docID, int offset ) {
 
    	//First check if this is a new document (docID different)
    	if(docID!=lastDocID){
    		lastToken=null;
    		lastTokenOffset=0;
    		lastDocID=docID;
    	}
    	
    	//If this is not the first token
    	if(lastToken!=null){
    		
    		String biword=lastToken+" "+token;
    		int biwordOffset=lastTokenOffset;
    		
		PostingsList list=index.get(biword);
		
		if(list==null){
			list=new PostingsList();
			index.put(biword,list);
		}
		//If there is no entry for the docID
		if(!list.checkContains(docID)){
			PostingsEntry newEntry=new PostingsEntry(docID);
			newEntry.setDocName(this.docIDs.get(""+docID));
			//System.err.println(this.docIDs.get(""+docID));
			newEntry.addOffset(biwordOffset);
			list.addEntry(newEntry);	
		}
		//Else if there already is a entry just add the offset
		else{
			list.addOffsetToLastEntry(biwordOffset); 
		}
	}
	lastToken=token;
	lastTokenOffset=offset;
    }
    


    /**
     *  Returns all the words in the index.
     */
    public Iterator<String> getDictionary() {
	Set<String> keySet=index.keySet();
	Iterator<String> it=keySet.iterator();
	return it;
    }


    /**
     *  Returns the postings for a specific term, or null
     *  if the term is not in the index.
     */
    public PostingsList getPostings( String token ) {
	PostingsList list=index.get(token);
	return list;
    }


    /**
     *  Searches the index for postings matching the query.
     */
    public PostingsList search( Query query, int queryType, int rankingType, int structureType ) {
	
    	    
    	if(queryType==Index.RANKED_QUERY){
    		PostingsList res=this.fastCosineScore(query,0);
    		return res;
	}
	else{
		System.err.println("ERROR: QueryType not recognized or unimplemented");
		return null;
	}
    }

    /**
    	Function to calculate the cosine score and return the top
    	K results
    */
    private PostingsList fastCosineScore(Query q, int K){
    	    
    	    HashMap<String,Double> scores=new HashMap<String,Double>();
    	    LinkedList<String> terms=q.terms;
    	    LinkedList<Double> termWeights=q.weights;
    	    
    	    int nrDocsInCorpus=this.docIDs.keySet().size();
    	    
    	    //First add up all scores
    	    for(int i=0;i<terms.size();i++){
    	    	
    	    	String term=terms.get(i);
    	    	double termWeight=termWeights.get(i);
    	    	    
    	    	PostingsList curList=index.get(term);
    	    	int dft=curList.size();
    	    	double termIDF=Math.log((double)nrDocsInCorpus/(double)dft);
    	    	
    	    	//Implementation of optimization
    	    	if(OPTIMIZATION){
    	    		//If the termIDF is less than the threshold skip this term
    	    		if(termIDF<IDF_THRESHOLD){
    	    			continue;	
    	    		}
    	    	}
    	    	
    	    	System.err.println("Term:"+term);
    	    	
    	    	//Multiply the termWeight to the termIDF
    	    	termIDF=termIDF*termWeight;
    	    	
    	    	//System.err.println("Term:"+term);
    	    	//System.err.println("dft:"+dft);
    	    	//System.err.println("TermIDF:"+termIDF);
    	    	
    	    	Iterator<PostingsEntry> it=curList.iterator();
    	    	PostingsEntry curEnt=null;
    	    	
    	    	while(it.hasNext()){
    	    	
    	    		curEnt=it.next();
    	    		double tf=(double)curEnt.getOffsets().size();
    	    		double tf_idf=tf*termIDF;
    	    		Double curScore=scores.get(""+curEnt.getDocID());
    	    		if(curScore==null){
    	    			curScore=tf_idf;	
    	    		}
    	    		else{
    	    			curScore=curScore+tf_idf;	
    	    		}
    	    		scores.put(""+curEnt.getDocID(),curScore);
    	    		
    	    	}
    	    	
    	    }
    	    //Postingslist to save all entries
    	    PostingsList finalList=new PostingsList();
    	    //Now we divide all scores by their docLength
    	    Iterator<String> keys=scores.keySet().iterator();
    	    while(keys.hasNext()){
    	    
    	    	    String curKey=keys.next();
    	    	    Integer docLength=this.docLengths.get(curKey);
    	    	    Double curScore=scores.get(curKey);
    	    	    int docID=Integer.parseInt(curKey);
    	    	    double finalScore=curScore/(double)docLength;
    	    	    PostingsEntry ent=new PostingsEntry(docID);
    	    	    ent.setScore(finalScore);
    	    	    finalList.addEntry(ent);
    	    }
    	    
    	    //When all entries are added we sort the finalList and pick out
    	    //the top K entries
    	    finalList.sortPostingsList();
    	    if(finalList.size()<K || K==0){
    	    	return finalList;	    
    	    }
    	    PostingsList retList=new PostingsList();
    	    for(int i=0;i<K;i++){
    	    	retList.addEntry(finalList.get(i));	    
    	    }
    	    return retList;
    }
    

    

    



    	    

    /**
     *  No need for cleanup in a HashedIndex.
     */
    public void cleanup() {
    }
}