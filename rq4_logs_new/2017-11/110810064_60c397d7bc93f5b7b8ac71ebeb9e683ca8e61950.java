package com.makebono.algorithms.computervision.colorindexing;

/** 
 * @ClassName: ColorHistogram 
 * @Description: Generate color histogram, for performance, the size of histogram is 8x8x8 for RGB channels.
 * @author makebono
 * @date 2017年11月27日 下午3:14:26 
 *  
 */
public class ColorHistogram {
    public static int[][][] generate(final int[][][] input) {
        final int[][][] histogram = new int[8][8][8];

        for (int i = 0; i < input.length; i++) {
            for (int o = 0; o < input[0].length; o++) {
                if (input[i][o][0] >= 20 && input[i][o][1] >= 20 && input[i][o][2] >= 20) {
                    int r = (int) (input[i][o][0] / 31.875) - 1;
                    int g = (int) (input[i][o][1] / 31.875) - 1;
                    int b = (int) (input[i][o][2] / 31.875) - 1;

                    if (r < 0) {
                        r = 0;
                    }

                    if (g < 0) {
                        g = 0;
                    }

                    if (b < 0) {
                        b = 0;
                    }

                    histogram[r][g][b] += 1;
                }
            }
        }

        return histogram;
    }
}