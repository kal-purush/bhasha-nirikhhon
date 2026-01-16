package com.vmelnyk.geeks4geeks._20240117WaterThePlants;

import java.util.Arrays;

class Solution {
  int min_sprinklers(int[] gallery, int n) {
    // code here
    int[] sprinklersCoverage = new int[n];
    Arrays.fill(sprinklersCoverage, -1);
    for (int i = 0; i < n; i++) {
      if (gallery[i] != -1) {
        int min = Math.max(0, i - gallery[i]);
        int max = Math.min(n - 1, i + gallery[i]);
        // System.out.println(i + "," + min + "," + max);
        for (int j = min; j <= max; j++) {
          sprinklersCoverage[j] = Math.max(sprinklersCoverage[j], max);
        }
      }
    }

    // System.out.println(" gallery:" + Arrays.toString(gallery));
    // System.out.println("coverage:" + Arrays.toString(sprinklersCoverage));

    int minSprinklersCount = 0;
    for (int i = 0; i < n; i++) {
      if (sprinklersCoverage[i] == -1) {
        minSprinklersCount = -1;
        break;
      }

      if (i <= sprinklersCoverage[i]) {
        minSprinklersCount++;
      }
      i = sprinklersCoverage[i];
    }
    return minSprinklersCount;
  }
}