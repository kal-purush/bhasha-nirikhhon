package br.dev.ferreiras.challenges.leetCode;

import java.util.Arrays;

/*
You start with an initial power of power, an initial score of 0, and a bag of tokens given
as an integer array tokens, where each tokens[i] donates the value of token i.
Your goal is to maximize the total score by strategically playing these tokens. In one move,
you can play an unplayed token in one of the two ways (but not both for the same token):

Face-up: If your current power is at least tokens[i], you may play token i, losing tokens[i]
power and gaining 1 score.
Face-down: If your current score is at least 1, you may play token i, gaining tokens[i] power
and losing 1 score.
Return the maximum possible score you can achieve after playing any number of tokens.
Example 1:
Input: tokens = [100], power = 50
Output: 0
Explanation:
Since your score is 0 initially, you cannot play the token face-down.
You also cannot play it face-up since your power (50) is less than tokens[0] (100).
Example 2:
Input: tokens = [200,100], power = 150
Output: 1
Explanation:
Play token1 (100) face-up, reducing your power to 50 and increasing your score to 1.
There is no need to play token0, since you cannot play it face-up to add to your score.
The maximum score achievable is 1.
Example 3:
Input:
tokens = [100,200,300,400], power = 200

Output: 2
Explanation:
Play the tokens in this order to get a score of 2:

Play token0 (100) face-up, reducing power to 100 and increasing score to 1.
Play token3 (400) face-down, increasing power to 500 and reducing score to 0.
Play token1 (200) face-up, reducing power to 300 and increasing score to 1.
Play token2 (300) face-up, reducing power to 0 and increasing score to 2.
The maximum score achievable is 2.
 */
public class BagOfTokens {
  public static void main(String[] args) {
    int[] tokens = {200, 100, 300, 400};
    int power = 200;
    int response = bagOfTokensScore(tokens, power);
    System.out.println(response);
  }

  public static int bagOfTokensScore(int[] tokens, int power) {

    Arrays.sort(tokens);
    int size = tokens.length;
    int rightPointer = size - 1;
    int leftPointer = 0;
    int currentScore = 0, maximumScore = 0;
    while (leftPointer <= rightPointer) {
      if (power >= tokens[leftPointer]) {
        power = power - tokens[leftPointer];
        currentScore++;
        leftPointer++;
        maximumScore = Math.max(maximumScore, currentScore);
      } else if (currentScore > 0) {
        power = power + tokens[rightPointer];
        currentScore--;
        rightPointer--;
      } else {
        break;
      }
    }
    return maximumScore;
  }

}