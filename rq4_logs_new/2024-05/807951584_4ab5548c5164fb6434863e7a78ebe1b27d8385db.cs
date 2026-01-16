using System;
using System.Collections.Generic;

public class TopVotedCandidate {
    private int[] times;
    private List<int> leaders;

    public TopVotedCandidate(int[] persons, int[] times) {
        this.times = times;
        leaders = new List<int>();
        Dictionary<int, int> voteCount = new Dictionary<int, int>();
        int currentLeader = -1;

        for (int i = 0; i < persons.Length; i++) {
            if (!voteCount.ContainsKey(persons[i])) {
                voteCount[persons[i]] = 0;
            }
            voteCount[persons[i]]++;

            if (currentLeader == -1 || voteCount[persons[i]] >= voteCount[currentLeader]) {
                if (currentLeader != persons[i] || voteCount[persons[i]] > voteCount[currentLeader]) {
                    currentLeader = persons[i];
                }
            }
            leaders.Add(currentLeader);
        }
    }

    public int q(int t) {
        int left = 0;
        int right = times.Length - 1;

        // Binary search to find the rightmost time less than or equal to t
        while (left < right) {
            int mid = (left + right + 1) / 2;
            if (times[mid] <= t) {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        return leaders[left];
    }
}

public class Program {
    public static void Main() {
        TopVotedCandidate topVotedCandidate = new TopVotedCandidate(
            new int[] { 0, 1, 1, 0, 0, 1, 0 }, 
            new int[] { 0, 5, 10, 15, 20, 25, 30 }
        );

        Console.WriteLine(topVotedCandidate.q(3));  // Output: 0
        Console.WriteLine(topVotedCandidate.q(12)); // Output: 1
        Console.WriteLine(topVotedCandidate.q(25)); // Output: 1
        Console.WriteLine(topVotedCandidate.q(15)); // Output: 0
        Console.WriteLine(topVotedCandidate.q(24)); // Output: 0
        Console.WriteLine(topVotedCandidate.q(8));  // Output: 1
    }
}