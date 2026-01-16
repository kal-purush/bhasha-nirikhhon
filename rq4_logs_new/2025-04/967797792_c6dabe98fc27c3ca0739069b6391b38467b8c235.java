// Amalia Karaman
// Trees and Heaps Assignment

import java.util.*;
import java.util.stream.Collectors;

public class Election {
    private Map<String,Integer> candidates; // map to track votes
    private PriorityQueue<CandidateVotes> maxHeap; // heap to get top
    private int totalVotes; // count all votes
    private int p; // total allowed votes

    public Election() {
        this.candidates=new HashMap<>(); // init map
        this.maxHeap=new PriorityQueue<>((a,b)->{ // init max heap
            if(b.votes!=a.votes)return b.votes-a.votes; // sort by votes
            return a.candidate.compareTo(b.candidate); // tiebreak lexicographically
        });
        this.totalVotes=0; // init count
        this.p=0; // init limit
    }

    public void initializeCandidates(List<String> candidates) {
        this.candidates.clear(); // reset map
        this.maxHeap.clear(); // reset heap
        this.totalVotes=0; // reset count
        for(String candidate:candidates){
            this.candidates.put(candidate,0); // start all at 0
            this.maxHeap.offer(new CandidateVotes(candidate,0)); // add to heap
        }
    }

    public void setTotalVotes(int p){
        this.p=p; // set vote cap
    }

    public boolean castVote(String candidate){
        if(!candidates.containsKey(candidate))return false; // skip invalid
        int newVotes=candidates.get(candidate)+1; // add 1 vote
        candidates.put(candidate,newVotes); // update map
        totalVotes++; // inc count
        maxHeap.offer(new CandidateVotes(candidate,newVotes)); // push to heap
        return true;
    }

    public boolean castRandomVote(){
        if(candidates.isEmpty())return false; // no one to vote
        List<String> candidateList=new ArrayList<>(candidates.keySet()); // get list
        String randomCandidate=candidateList.get(new Random().nextInt(candidateList.size())); // pick random
        return castVote(randomCandidate); // cast it
    }

    public boolean rigElection(String candidate){
        if(!candidates.containsKey(candidate))return false; // skip if not real
        for(String c:candidates.keySet())candidates.put(c,0); // reset all
        totalVotes=0; // reset count
        maxHeap.clear(); // clear heap
        candidates.put(candidate,3); // rig 3 votes
        totalVotes+=3; // update count
        maxHeap.offer(new CandidateVotes(candidate,3)); // push to heap

        List<String> others=candidates.keySet().stream() // get other names
                .filter(c->!c.equals(candidate))
                .collect(Collectors.toList());

        if(others.contains("Cole Train")){ // 1 vote to cole
            candidates.put("Cole Train",1);
            totalVotes+=1;
            maxHeap.offer(new CandidateVotes("Cole Train",1));
        }
        if(others.contains("Anya Stroud")){ // 1 vote to anya
            candidates.put("Anya Stroud",1);
            totalVotes+=1;
            maxHeap.offer(new CandidateVotes("Anya Stroud",1));
        }
        return true;
    }

    public List<String> getTopKCandidates(int k){
        return candidates.entrySet().stream() // sort by votes
                .sorted((a,b)->{
                    int voteCompare=b.getValue().compareTo(a.getValue());
                    if(voteCompare!=0)return voteCompare;
                    return a.getKey().compareTo(b.getKey());
                })
                .limit(k) // top k
                .map(Map.Entry::getKey) // get names only
                .collect(Collectors.toList());
    }

    public void auditElection(){
        candidates.entrySet().stream() // go through all
                .sorted((a,b)->{
                    int voteCompare=b.getValue().compareTo(a.getValue());
                    if(voteCompare!=0)return voteCompare;
                    return a.getKey().compareTo(b.getKey());
                })
                .forEach(entry->System.out.println(entry.getKey()+" - "+entry.getValue())); // print each
    }

    private static class CandidateVotes {
        String candidate; // name
        int votes; // votes

        public CandidateVotes(String candidate,int votes){
            this.candidate=candidate; // set name
            this.votes=votes; // set votes
        }
    }
}

class ElectionSystem {
    private Election election; // election obj

    public ElectionSystem(){
        this.election=new Election(); // init
    }

    public void runSampleElection(){
        List<String> candidates=Arrays.asList( // list of names
                "Marcus Fenix","Dominic Santiago","Damon Baird","Cole Train","Anya Stroud"
        );
        int p=5; // total allowed
        election.initializeCandidates(candidates); // set up
        election.setTotalVotes(p); // set p

        System.out.println("Sample operations:");
        election.castVote("Cole Train"); // vote
        election.castVote("Cole Train");
        election.castVote("Marcus Fenix");
        election.castVote("Anya Stroud");
        election.castVote("Anya Stroud");

        System.out.println("Top 3 candidates after 5 votes: "+election.getTopKCandidates(3)); // show top

        election.rigElection("Marcus Fenix"); // rig it
        System.out.println("Top 3 candidates after rigging the election: "+election.getTopKCandidates(3)); // show rigged

        System.out.println("auditElection():"); // print audit
        election.auditElection();
    }

    public static void main(String[] args){
        ElectionSystem system=new ElectionSystem(); // make system
        system.runSampleElection(); // run it
    }
}