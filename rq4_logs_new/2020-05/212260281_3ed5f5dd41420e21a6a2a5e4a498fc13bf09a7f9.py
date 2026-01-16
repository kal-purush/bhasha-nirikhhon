class Solution:
    def isHappy(self, n: int) -> bool:
        '''
        Time: O(N LOG N), the inner loop iterations the length of N and we do that until we find 1 (N)
        Space: O(N), dictionary increases every iteration
        '''
        # Hashtable
        seen = {}

        # Iterate until we find 1
        while n != 1:
            # current value (temp)
            current = n

            # clear sum
            sum = 0

            # Generate the sum
            while current != 0:
                sum += (current % 10) ** 2
                current //= 10

            # Check the hashtable to see if we have already seen the sum
            # if seen already we are looping and the number is not happy
            if sum in seen:
                return False

            # add to the hashtable
            seen[sum] = 1
            n = sum

        return True