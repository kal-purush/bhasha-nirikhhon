package server;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SwearFilter {
    private Set<String> bannedWords;
    
    public SwearFilter() {
        // Initialize with a list of banned words
        bannedWords = new HashSet<>(Arrays.asList(
            "badword", "swear", "offensive", "inappropriate", "curse"
            // Add more banned words as needed
        ));
    }
    
    public String filter(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        
        String[] words = text.split("\\s+");
        StringBuilder filtered = new StringBuilder();
        
        for (String word : words) {
            // Check if the word (lowercase) is in the banned list
            if (bannedWords.contains(word.toLowerCase())) {
                // Replace with asterisks
                filtered.append("*".repeat(word.length()));
            } else {
                filtered.append(word);
            }
            filtered.append(" ");
        }
        
        // Trim the trailing space and return
        return filtered.toString().trim();
    }
    
    // Method to add additional banned words at runtime
    public void addBannedWord(String word) {
        bannedWords.add(word.toLowerCase());
    }
}