public class Storage {
    Storage[] children;
    boolean isWord;

    public Storage(boolean isWord) {
        this.isWord = isWord;
        this.children = new Storage[26];
    }

    public Storage() {
        this.isWord = false;
        this.children = new Storage[26];
    }

    public void setChild(String value) {
        System.out.println(value);
        if (value.isEmpty()) {
            isWord = true;
            return;
        }
        int val = (int) value.charAt(0) - 65;
        if (children[val] == null) {
            children[val] = new Storage();
        }
        String newValue;
        if (value.length() == 1) {
            newValue = "";
        }
        else {
            newValue = value.substring(1);
        }
        children[val].setChild(newValue);
    }

    public boolean checkWord(String value) {
        if (value.isEmpty()) {
            return false;
        }
        String word;
        if (value.length() == 1) {
            word = "";
        }
        else {
            word = value.substring(1);
        }
        return children[(int) value.charAt(0)].checkWord(word);
    }
}