package medium;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * https://leetcode.com/problems/remove-sub-folders-from-the-filesystem/
 *
 * Given a list of folders folder, return the folders after removing all sub-folders in those folders. You may return the answer in any order.
 *
 * If a folder[i] is located within another folder[j], it is called a sub-folder of it.
 *
 * The format of a path is one or more concatenated strings of the form: '/' followed by one or more lowercase English letters.
 *
 * For example, "/leetcode" and "/leetcode/problems" are valid paths while an empty string and "/" are not.
 *
 * TC: O(nlogn + n * m)
 * SC: O(n)
 */
public class Remove_Sub_Folders_from_the_Filesystem {
    public List<String> removeSubfolders(String[] folder) {
        List<String> ret = new ArrayList<>();

        Arrays.sort(folder, Comparator.comparing((str1) -> str1.length()));

        TrieNode root = new TrieNode("/");

        for(int i = 0; i < folder.length; i++) {
            //System.out.println(folder[i]);
            String[] folders = folder[i].split("/");
            if (!root.containsChild(folders[1])) {
                // build the complete path
                //System.out.println(folder[i] + " is not present. Creating the tree.");
                buildTrieFromRoot(root, folders);
                ret.add(folder[i]);
            } else {
                int j = 2;
                TrieNode temp = root.getChildNode(folders[1]);
                while(j < folders.length) {
                    String cur = folders[j];
                    if (temp.map.containsKey(cur)) {
                        temp = temp.getChildNode(cur);
                    } else {
                        if(temp.map.keySet().size() == 0) {
                            // there are no other children. Don't create one.
                            break;
                        } else {
                            // There is another child. Create one alog with it.
                            buildTrieFromRoot(temp, Arrays.copyOfRange(folders, j-1, folders.length));
                            ret.add(folder[i]);
                            break;
                        }
                    }
                    j++;
                }
            }
        }

        return ret;
    }



    // Constructs patch without checking any condition.
    public void buildTrieFromRoot(TrieNode parent, String[] folders) {
        for(int i = 1; i < folders.length; i++) {
            if(parent == null) return;
            if(!parent.containsChild(folders[i])) {
                parent.insert(folders[i]);
            }
            parent = parent.getChildNode(folders[i]);
        }
    }

    class TrieNode {
        public String name;
        public Map<String, TrieNode> map;

        public TrieNode(String nodeVal) {
            this.name = nodeVal;
            map = new HashMap<>();
        }

        public void insert(String child) {
            TrieNode node = new TrieNode(child);
            map.put(child, node);
        }

        public boolean containsChild(String child) {
            return map.containsKey(child);
        }

        public TrieNode getChildNode(String child) {
            return map.getOrDefault(child, null);
        }
    }
}