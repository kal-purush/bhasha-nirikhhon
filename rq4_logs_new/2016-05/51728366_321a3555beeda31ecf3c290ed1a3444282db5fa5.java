/*
   Given an unsorted array return whether an increasing subsequence of length 3 exists or not in the array.
   
   Formally the function should:
   Return true if there exists i, j, k 
   such that arr[i] < arr[j] < arr[k] given 0 <= i < j < k <= n-1 else return false.
   Your algorithm should run in O(n) time complexity and O(1) space complexity.
   
   Examples:
   Given [1, 5, 3, 2, 5,3] // 1 < 2 < 3
   return true.
   
   Given [5, 5, 3, 4, 4],
   return false.
*/



public class IncreasingTriplet {
    public boolean increasingTriplet(int[] nums) {
        if(nums.length < 3)
            return false;
        int firstVal = nums[0];
        int secondVal = Integer.MAX_VALUE;

        for(int i=1; i<nums.length; i++){
            if(nums[i] == firstVal)
                continue;
            else if(nums[i] < firstVal){
                firstVal = nums[i];
            }
            else{ 
                if(nums[i] > secondVal)
                    return true;
                else {
                    secondVal = nums[i];
                }
            }
        }
        return false;
    }
    //more concise code:
    public boolean increasingTriplet2(int[] nums) {
        int firstVal = Integer.MAX_VALUE;
        int secondVal = Integer.MAX_VALUE;

        for(int num: nums){
            if(num <= firstVal){
                firstVal = num;
            }
            else if(num > secondVal)
                return true;
            else
                secondVal = num;
        }
        return false;
    }
}


/*
for [3,2,5,0,5,4,5]

step 1:

| 3 ||   |

step 2:

| 2 ||   |
| 3 ||   |

step 3:

| 2 || 5 |
| 3 || 5 |

step 4:

| 0 ||   |
| 2 || 5 |
| 3 || 5 |

// when loop to the second 5, continue
step 5:
| 0 || 4 |
| 2 || 4 |
| 3 || 4 |

step 6:
| 0 || 4 | //return true when loop to the third 5;
| 2 || 4 |
| 3 || 4 |

*/
//notes: at first I was try to use stack. But it turs out a linked list of array or a linked list of stack will be good for this problem.
//so I tried a linked list of array LinkedList<int []> ll = new LinkedList<int []>(); the array length is 2; 
//I got the result, but the performance isn't good enough. After analysis for my solution, I figure out linked list isn't necessary. Because
//when the latest first value(smallest one) got the second value, the previous value pairs are redundant.
// after modification, my algorithm changed to use an array of length n, an index, an second value as extra space.
/*
for [3,2,5,0,5,4,5]
step 1: (loop to 3)
| 3 ||   ||   ||   ||   ||   ||   ||   ||   |.....|   | //same length with input array
secondValue = MAXINT
index = 0;

step 2: (loop to 2)
| 3 || 2 ||   ||   ||   ||   ||   ||   ||   |.....|   | //same length with input array
secondValue = MAXINT
index = 1;

step 3: (loop to 5)
| 3 || 2 ||   ||   ||   ||   ||   ||   ||   |.....|   | //same length with input array
secondValue = 5
index = 1;

step 4: (loop to 0)
| 3 || 2 || 0 ||   ||   ||   ||   ||   ||   |.....|   | //same length with input array
secondValue = 5
index = 2;


// when loop to the second 5, continue

step 5: (loop to 4)
| 3 || 2 || 0 ||   ||   ||   ||   ||   ||   |.....|   | //same length with input array
secondValue = 4
index = 2;

step 5: (loop to 5)
| 3 || 2 || 0 ||   ||   ||   ||   ||   ||   |.....|   | //same length with input array
secondValue = 4 // return true when compare 5 with secondValue
index = 2;
*/


   //After writting this, I realize the array isn't necessary too. Try to use a firstValue variable instead.
   //So the meaning of firstValue and secondValue are: 
   //firstValue = so far best candidate of end element of a one-cell subsequence to form a triplet subsequence
   //secondValue = so far best candidate of end element of a two-cell subsequence to form a triplet subsequence
/*
for [3,2,5,0,5,4,5]
step 1: (loop to 3)
firstValue = 3
secondValue = MAXINT

step 2: (loop to 2)
firstValue = 2
secondValue = MAXINT

step 3: (loop to 5)
firstValue = 2
secondValue = 5

step 4: (loop to 0)
firstValue = 0
secondValue = 5


// when loop to the second 5, continue

step 5: (loop to 4)
firstValue = 0
secondValue = 4

step 5: (loop to 5)
firstValue = 0
secondValue = 4 // return true when compare 5 with secondValue

*/