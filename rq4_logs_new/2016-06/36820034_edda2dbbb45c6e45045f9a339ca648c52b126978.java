package org.cmucreatelab.tasota.airprototype.classes.aqi_scales;

import android.util.Log;
import org.cmucreatelab.tasota.airprototype.R;
import org.cmucreatelab.tasota.airprototype.helpers.static_classes.AqiConverter;
import org.cmucreatelab.tasota.airprototype.helpers.static_classes.Constants;

/**
 * Created by mike on 6/9/16.
 */
public class AQIReading extends Scalable {

    // static class attributes
    private static final String[] descriptions = {
            "Air quality is considered satisfactory, and air pollution poses little or no risk.",
            "Air quality is acceptable; however, for some pollutants there may be a moderate " +
                    "health concern for a very small number of people. For example, people " +
                    "who are unusually sensitive to ozone may experience respiratory symptoms.",
            "Although general public is not likely to be affected at this AQI range, people " +
                    "with lung disease, older adults and children are at a greater risk from " +
                    "exposure to ozone, whereas persons with heart and lung disease, older " +
                    "adults and children are at greater risk from the presence of particles " +
                    "in the air.",
            "Everyone may begin to experience some adverse health effects, and members of the " +
                    "sensitive groups may experience more serious effects.",
            "This would trigger a health alert signifying that everyone may experience more " +
                    "serious health effects.",
            "This would trigger a health warning of emergency conditions. The entire " +
                    "population is more likely to be affected."
    };
    private static final String[] titles = {
            "Good", "Moderate", "Unhealthy for Sensitive Groups",
            "Unhealthy", "Very Unhealthy", "Hazardous"
    };
    private static final String[] aqiColors = {
            "#a3ba5c", "#e9b642", "#e98c37",
            "#e24f36", "#b54382", "#b22651"
    };
    private static final String[] aqiFontColors = {
            "#192015", "#2a1e11", "#261705",
            "#330004", "#2d0d18", "#28060b"
    };
    private static final int[] aqiDrawableGradients = {
            R.drawable.gradient_0, R.drawable.gradient_1, R.drawable.gradient_2,
            R.drawable.gradient_3, R.drawable.gradient_4, R.drawable.gradient_5
    };
    private static final int[] ranges = {
            50, 100, 150,
            200, 300
    };
    // class attributes
    private final double reading;
    private final int index;
    // getters
    public String getFontColor() { return aqiFontColors[this.index]; }
    public int getDrawableGradient() { return aqiDrawableGradients[this.index]; }
    public String getDescription() { return descriptions[this.index]; }
    public String getColor() { return aqiColors[this.index]; }
    public String getTitle() { return titles[this.index]; }


    public AQIReading(double reading) {
        this.reading = reading;
        this.index = getIndexFromReading(AqiConverter.microgramsToAqi(this.reading));
    }


    public int getIndexFromReading(double reading) {
        if (reading < 0) {
            return -1;
        }
        int i;
        for (i=0;i<ranges.length;i++) {
            if (reading < ranges[i]) {
                return i;
            }
        }
        return -1;
    }


    public String getRangeFromIndex(int index) {
        String result;
        if (index < 0) {
            Log.e(Constants.LOG_TAG, "getRangeFromIndex received index < 0.");
            result = "";
        } else if (index == 0) {
            result = "0-" + ranges[0];
        } else if (index == 5) {
            result = ranges[4] + "+";
        } else {
            result = ranges[index-1] + "-" + ranges[index];
        }
        return result;
    }

}