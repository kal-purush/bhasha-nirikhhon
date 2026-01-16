package processing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import core.TagLast;
import core.TagsToCSV;

public class WeightingLast {

	public void byWeightedMean(List<TagLast> tags, String prefix, Boolean verbose)
	{
	    /////////////////////////////////
	    // Variables
	    Map<String, Double> tag_words = new HashMap<String, Double>();
	    
	  	TagsToCSV writer;
	  	List<String> weights = new ArrayList<String>();
	  	Set<String> temp = new HashSet<String>();
	    
	    long listeners, playcount, lastfmweight;
	    String key;
	    double importance, max_w = 0;
	    double q_listeners = 1, q_playlist = 1;
	    double value;
	    Boolean print_groups;
	    
		/////////////////////////////////
		// Configuration
	    
		// Set weights for the weighted normalized weight for each tag/song pair
		// Listeners
		q_listeners = 1;
		// Playcount
		q_playlist = 1;
		
		// Verbose
		print_groups = verbose;	

		/////////////////////////////////
		// Algorithm	

	    // Compute a weighted normalized weight for each tag/song pair
	    for(TagLast t: tags)
	    {
	    	listeners = t.getListeners();
	    	playcount = t.getPlaycount();
	    	lastfmweight = t.getLastFMWeight();
	    	
	    	// If the playcount is negative I ignore it
	    	if(playcount <= 0) 
    		{
    			q_playlist = 0;
    			// If playcount is 0 i get a weight of NaN
    			playcount = 1;
    		}
	    		    	
	    	importance = lastfmweight*(q_listeners*Math.log(listeners)+q_playlist*Math.log(playcount));
	    	
	    	t.setImportance(importance); 
	    	
	    }
	    
	    // Summing up the weights
	    for(int i = 0;i < tags.size(); i++)
	    {	    	    		
    		importance = tags.get(i).getImportance();
    		key = tags.get(i).getTagName();

    		if(tag_words.containsKey(key))
    		{
    			value = tag_words.get(key);
    			
    			// Sum up the weight over all songs
    			tag_words.put(key, value + importance);
    		}
    		else
    		{
    			tag_words.put(key, importance);
    		}
	    }
	    
	    // maximum importance of tag_words
	    for(String s: tag_words.keySet())
	    {
	    	if(tag_words.get(s) > max_w)
	    	{
	    		max_w = tag_words.get(s);
	    	}
	    }
	    
	    // normalizing tags
	    for(TagLast t:tags)
	    {
	    	importance = tag_words.get(t.getTagName());
	    	
	    	t.setImportance(importance/max_w);
	    	
	    	weights.add(t.getTagName()+","+importance/max_w);
	    }
	    
	    // Write temp files
	    if(print_groups) 
    	{	
	    	// Remove duplicates and sort
	    	temp.addAll(weights);
	    	
	    	weights.clear();
	    	
	    	weights.addAll(temp);
	    	Collections.sort(weights);
	    	
	    	writer = new TagsToCSV("importance_"+prefix+".csv");
	    	writer.writeLines(weights,"importance");
    	}
    	
  
	    tag_words = null;
	}
}