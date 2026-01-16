import sys
from typing import List
import math

def ASCII_art(T: int, test_cases: List[int]) -> List[str]:
    results = []

    for t, N in enumerate(test_cases, 1):
        i = 1
        while N > 26 * i:
            N = N - 26 * i
            i += 1
        letter_idx = math.ceil(N / i) - 1
        results.append(f"Case #{t}: {chr(ord('A') + letter_idx)}")

    return results

def main():
    T = int(sys.stdin.readline().rstrip())
    test_cases = []
    for _ in range(T):
        N = int(sys.stdin.readline().rstrip())
        test_cases.append(N)

    results = ASCII_art(T, test_cases)

    for result in results:
        print(result)

if __name__ == '__main__':
    main()