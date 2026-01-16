package sw19.moring03.paint;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.PointF;
import android.support.test.rule.ActivityTestRule;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import sw19.moring03.paint.tools.LineTool;
import sw19.moring03.paint.tools.PhotoTool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class StickerTest {
    private Canvas canvas;
    private Paint paint;

    @Rule
    public ActivityTestRule<MainActivity> activityTestRule = new ActivityTestRule<>(MainActivity.class);

    @Before
    public void startUp() {
        canvas = new Canvas();
        paint = new Paint();
    }

    @Test
    public void testStickerSimple() {
        Bitmap image = getBitmap();
        StickerTool tool = new StickerTool(image);
        tool.addPoint(new PointF(10, 10), 5);
        assertTrue(tool.draw(canvas, paint));
    }

    @Test
    public void testInvalidSticker() {
        StickerTool tool = new StickerTool(null);
        assertTrue(tool.draw(canvas, paint));
    }

    @Test
    public void testDrawOnSticker() {
        Bitmap image = getBitmap();
        StickerTool stickerTool = new StickerTool(image);
        stickerTool.addPoint(new PointF(10, 10), 5);
        assertTrue(stickerTool.draw(canvas, paint));

        LineTool lineTool = new LineTool(Color.BLACK, 5);
        lineTool.addPoint(new PointF(10, 10));

        assertFalse(lineTool.draw(canvas, paint));
    }

    private Bitmap getBitmap() {
        return BitmapFactory.decodeResource(activityTestRule.getActivity().getResources(), R.drawable.happy);
    }
}