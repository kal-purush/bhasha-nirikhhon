public class topKFrequentWords{
    public List<String> topKFrequntWords(int k,String[] keywords,Sting[] reviews){
        List<String> result = new ArrayList();
        Set<String> set = new HashSet(Arrays.asList(keywords));
        Map<String,Integer> map = new HashMap();

        for(String review:reviews){
            Set<String> added = new HashSet();
            String[] word = review.split("\\W");
            for(String w:word){
                if(set.contains(w)&&!added.contains(w)){
                    map.put(w,map.getOrDefault(w,0)+1);
                    added.add(w);
                }
            }
        }
        PriorityQueue<Map.Entry<String,Integer>> heap = new PriorityQueue<>((a,b)-> (a.getValue()==b.getValue())?a.getKet().compareTo(b.getKey()):
                b.getValue()-a.getValue());
        heap.addAll(map.entrySey());
        while(!heap.isEmpty()&&k-->0){
            result.add(heap.poll().getKey());
        }
        return result;
    }
}