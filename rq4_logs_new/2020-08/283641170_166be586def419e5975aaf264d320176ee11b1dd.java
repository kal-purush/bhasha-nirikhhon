class ReOrderLogFiles {
    public String[] reorderLogFiles(String[] logs) {

        Arrays.sort(logs,(log1,log2)->{
            String[] str1 = log1.split(" ",2);
            String[] str2 = log2.split(" ",2);

            boolean isdigit1 = Character.isDigit(str1[1].charAt(0));
            boolean isdigit2 = Character.isDigit(str2[1].charAt(0));

            if(!isdigit1 && !isdigit2){
                int comp = str1[1].compareTo(str2[1]);
                if(comp!=0)
                    return comp;
                return str1[0].compareTo(str2[0]);
            }

            return isdigit1?(isdigit2?0:1):-1;
        });
        return logs;
    }
}

//this is custom sort logic. input array of strings. sort by character and digit and identifier.
//used Arrays.sort function and passes two logs as input to sort.
//Arrays.sort time complexity O(nlogn)