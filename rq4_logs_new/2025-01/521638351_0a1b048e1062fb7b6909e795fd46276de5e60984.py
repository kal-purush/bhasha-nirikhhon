from typing import List
from collections import defaultdict, Counter
from itertools import combinations

class Solution:
    def mostVisitedPattern(self, username: List[str], timestamp: List[int], website: List[str]) -> List[str]:
        stats = zip(username, timestamp, website)
        stats = sorted(stats, key=lambda x: (x[0], x[1]))
        usersTable = defaultdict(list)

        for user, _, site in stats:
            usersTable[user].append(site)

        patterns = Counter()

        for user, sites in usersTable.items():
            sequences = Counter(set(combinations(sites, 3)))
            patterns.update(sequences)

        return max(sorted(patterns), key=patterns.get)

# TC: O(N^3 + M + NLogN)
# SC: O(N + M)