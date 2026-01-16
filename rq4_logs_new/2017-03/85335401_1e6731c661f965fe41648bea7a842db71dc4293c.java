package hakaton.cuckoobot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * @author Dmitry Tarasov
 *         Date: 03/18/2017
 *         Time: 08:54
 */
public class DataSource {
    
    Map<String,Map<String,Issue>> storage = new HashMap<String, Map<String, Issue>>();
    
    
    public Issue getIssue(String id, String groupId){
        return null;
    }
    
    public List<Issue> getIssueForGroup(String groupId){
        Map<String,Issue> group = storage.get(groupId);
        if (group==null){
            group = new TreeMap<String,Issue>();
            storage.put(groupId,group);
        }
        return new ArrayList<Issue>(group.values());
    }
    
    public Issue addIssue(Issue issue){
        Map<String,Issue> group = storage.get(issue.getOwner());
        if (group==null){
            group = new TreeMap<String, Issue>();
            storage.put(issue.getOwner(),group);
        }
        issue.setId(generateId());
        group.put(issue.getId(),issue);
        return issue;
    }
    
    private String generateId(){
        return System.currentTimeMillis()+"";
    }
}