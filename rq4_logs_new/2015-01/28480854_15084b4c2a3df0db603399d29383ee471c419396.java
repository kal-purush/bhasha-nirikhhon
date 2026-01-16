package org.opentex.jimel.image;

/**
 * Created by jdborowy on 25/12/2014.
 */
public abstract class AbstractImage implements Image {
    public static final int AlphaMask = 0xff000000;
    public static final int RedMask = 0x00ff0000;
    public static final int GreenMask = 0x0000ff00;
    public static final int BlueMask = 0x000000ff;

    protected int width;
    protected int height;
    protected final int channels = 3;
    protected final int dx = 1;
    protected int dy;
    protected String name;

    protected AbstractImage(int width, int height, int dy) {
        this.width = width;
        this.height = height;
        this.dy = dy;
    }

    public static final int getRedValue(int color) {
        return (color& AbstractImage.RedMask) >> 16;
    }

    public static final int getGreenValue(int color) {
        return (color& AbstractImage.GreenMask) >> 8;
    }

    public static final int getBlueValue(int color) {
        return color& AbstractImage.BlueMask;
    }

    public static final int getColor(int red, int green, int blue) {
        return AlphaMask | red << 16 | green << 8 | blue;
    }

    public final int getWidth() {
        return width;
    }

    public final int getHeight() {
        return height;
    }

    public final int getChannels() {
        return channels;
    }

    public final int getdx() {
        return dx;
    }

    public final int getdy() {
        return dy;
    }

    public final String getName() {
        return name;
    }

    public final String setName(String name) {
        return this.name = name;
    }

    public final void paste(Image image, int x0, int y0) {
        int[] data0 = this.getData();
        int[] data1 = image.getData();
        int dy0 = this.dy;
        int dy1 = image.getdy();
        int startIndex0 = this.getStartIndex() + y0*dy0 + x0;
        int startIndex1 = image.getStartIndex();

        for (int y = 0, yy = image.getHeight(), xx = image.getWidth(); y < yy; ++y) {
            int tmp0 = startIndex0 + y*dy0;
            int tmp1 = startIndex1 + y*dy1;
            for (int x = 0; x < xx; ++x) {
                data0[tmp0 + x] = data1[tmp1 + x];
            }
        }
    }

    public final void fill(int color, int x0, int y0, int width, int height) {
        int[] data0 = this.getData();
        int dy0 = this.dy;
        int startIndex0 = this.getStartIndex() + y0*dy0 + x0;

        for (int y = 0, yy = this.height, xx = this.height; y < yy; ++y) {
            int tmp0 = startIndex0 + y*dy0;
            for (int x = 0; x < xx; ++x) {
                data0[tmp0 + x] = color;
            }
        }
    }

    public final void fill(int color) {
        this.fill(color, 0, 0, this.width, this.height);
    }

    public final void paste(Image image, Image mask, int x0, int y0) {
        int[] data0 = this.getData();
        int[] data1 = image.getData();
        int[] dataMask = mask.getData();
        int dy0 = this.dy;
        int dy1 = image.getdy();
        int dyMask = mask.getdy();
        int startIndex0 = this.getStartIndex() + y0*dy0 + x0;
        int startIndex1 = image.getStartIndex();
        int startIndexMask = mask.getStartIndex();

        for (int y = 0, yy = image.getHeight(), xx = image.getWidth(); y < yy; ++y) {
            int tmp0 = startIndex0 + y*dy0;
            int tmp1 = startIndex1 + y*dy1;
            int tmpMask = startIndexMask + y*dyMask;
            for (int x = 0; x < xx; ++x) {
                int color0 = data0[tmp0 + x];
                int color1 = data1[tmp1 + x];
                int maskValue = dataMask[tmpMask + x];
                int aboveRed = AbstractImage.getRedValue(color1)*maskValue;
                int aboveGreen = AbstractImage.getGreenValue(color1)*maskValue;
                int aboveBlue = AbstractImage.getBlueValue(color1)*maskValue;
                int bellowRed = AbstractImage.getRedValue(color0)*(255 - maskValue);
                int bellowGreen = AbstractImage.getGreenValue(color0)*(255 - maskValue);
                int bellowBlue = AbstractImage.getBlueValue(color0)*(255 - maskValue);

                data0[tmp0 + x] = AbstractImage.getColor(
                        (aboveRed + bellowRed) >> 8,
                        (aboveGreen + bellowGreen) >> 8,
                        (aboveBlue + bellowBlue) >> 8);
            }
        }
    }

    public double distance(Image image) {
        int[] data0 = this.getData();
        int[] data1 = image.getData();
        int dy0 = this.dy;
        int dy1 = image.getdy();
        int startIndex0 = this.getStartIndex();
        int startIndex1 = image.getStartIndex();
        double distance = 0.;

        for (int y = 0, yy = image.getHeight(), xx = image.getWidth(); y < yy; ++y) {
            int tmp0 = startIndex0 + y*dy0;
            int tmp1 = startIndex1 + y*dy1;
            for (int x = 0; x < xx; ++x) {
                int color0 = data0[tmp0 + x];
                int color1 = data1[tmp1 + x];
                int dred = AbstractImage.getRedValue(color0) - AbstractImage.getRedValue(color1);
                int dgreen = AbstractImage.getGreenValue(color0) - AbstractImage.getGreenValue(color1);
                int dblue = AbstractImage.getBlueValue(color0) - AbstractImage.getBlueValue(color1);
                distance += dred*dred + dgreen*dgreen + dblue*dblue;
            }
        }

        return distance;
    }

    public double distance(Image image, Image mask) {
        int[] data0 = this.getData();
        int[] data1 = image.getData();
        int[] dataMask = mask.getData();
        int dy0 = this.dy;
        int dy1 = image.getdy();
        int dyMask = mask.getdy();
        int startIndex0 = this.getStartIndex();
        int startIndex1 = image.getStartIndex();
        int startIndexMask = mask.getStartIndex();
        double distance = 0.;

        for (int y = 0, yy = image.getHeight(), xx = image.getWidth(); y < yy; ++y) {
            int tmp0 = startIndex0 + y*dy0;
            int tmp1 = startIndex1 + y*dy1;
            int tmpMask = startIndexMask + y*dyMask;
            for (int x = 0; x < xx; ++x) {
                int color0 = data0[tmp0 + x];
                int color1 = data1[tmp1 + x];
                int maskValue = dataMask[tmpMask + x];
                int dred = AbstractImage.getRedValue(color0) - AbstractImage.getRedValue(color1);
                int dgreen = AbstractImage.getGreenValue(color0) - AbstractImage.getGreenValue(color1);
                int dblue = AbstractImage.getBlueValue(color0) - AbstractImage.getBlueValue(color1);
                distance += maskValue*(dred*dred + dgreen*dgreen + dblue*dblue);
            }
        }

        return distance;
    }


}