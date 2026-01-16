class HuffmanNode implements Comparable<HuffmanNode> {
    int freq;
    String str;
    HuffmanNode left,right;
    HuffmanNode(){
        freq=0;
    }
    HuffmanNode(String c,int freq,HuffmanNode left,HuffmanNode right){
        this.freq=freq;
        this.str=c;
        this.left=left;
        this.right=right;
    }
    public boolean isLeaf() {
        return right == null && left == null;
    }
    @Override
    public int compareTo(HuffmanNode HN){
        return this.freq-HN.freq;
    }
}