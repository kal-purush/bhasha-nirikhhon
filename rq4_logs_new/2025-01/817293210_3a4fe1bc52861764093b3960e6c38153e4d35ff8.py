from typing import List


def minExtraChar(self, s: str, dictionary: List[str]) -> int:
    dp = [0] * (len(s) + 1)
    st = set(dictionary)
    for i in range(1, len(s) + 1):
        dp[i] = dp[i - 1] + 1
        for j in range(1, i + 1):
            if s[i - j:i] in st:
                dp[i] = min(dp[i], dp[i - j])
    return dp[len(s)]


if __name__ == "__main__":
    dictionary=["leet","code","leetcode"]
    s="leetscode"
    print(minExtraChar(s,dictionary))