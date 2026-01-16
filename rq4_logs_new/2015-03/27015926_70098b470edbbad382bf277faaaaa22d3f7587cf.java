/******************************************************************************
 * Title: ImperialTallyData.java
 * Author: Hunter Schoonover
 * Date: 03/09/15
 *
 * Purpose:
 *
 * This class is used for storing, comparing, and calculating the imperial
 * tally data.
 *
 */

//-----------------------------------------------------------------------------

package com.yaboosh.ybtech.lasertally;

//-----------------------------------------------------------------------------
//-----------------------------------------------------------------------------
// class ImperialTallyData
//

import java.io.File;
import java.text.DecimalFormat;

public class ImperialTallyData extends TallyData {

    //-----------------------------------------------------------------------------
    // ImperialTallyData::ImperialTallyData (constructor)
    //

    public ImperialTallyData(SharedSettings pSet, JobInfo pJobInfo)
    {

        super(pSet, pJobInfo);

        LOG_TAG = "ImperialTallyData";

        decFormat = new DecimalFormat("#.00");

        thisClassUnitSystem = Keys.IMPERIAL_MODE;

        conversionFactor = 1;

    }//end of ImperialTallyData::ImperialTallyData(constructor)
    //-----------------------------------------------------------------------------

    //-----------------------------------------------------------------------------
    // ImperialTallyData::setJobInfoVariables
    //
    // Sets variables to values stored in JobInfo.
    //
    // Should be called every time JobInfo changes.
    //

    void setJobInfoVariables()
    {

        filePath = jobInfo.getCurrentJobDirectoryPath() + File.separator
                                                        + jobInfo.getJobName()
                                                        + " ~ TallyData ~ Imperial.csv";

        setAdjustmentValue(Double.parseDouble(jobInfo.getImperialAdjustment()));

        //hss wip//--should be class/unit system specific
        setTallyGoal(jobInfo.getTallyGoal());

    }//end of ImperialTallyData::setJobInfoVariables
    //-----------------------------------------------------------------------------

    //-----------------------------------------------------------------------------
    // ImperialTallyData::setSharedSettingsVariables
    //
    // Sets variables to values stored in SharedSettings.
    //
    // Should be called every time SharedSettings changes.
    //

    void setSharedSettingsVariables()
    {

        //hss wip//--should be class/unit system specific
        calibrationValue = Double.parseDouble(sharedSettings.getCalibrationValue());
        maxAllowed = Double.parseDouble(sharedSettings.getMaximumMeasurementAllowed());
        minAllowed = Double.parseDouble(sharedSettings.getMinimumMeasurementAllowed());

    }//end of ImperialTallyData::setSharedSettingsVariables
    //-----------------------------------------------------------------------------

}//end of class ImperialTallyData
//-----------------------------------------------------------------------------
//-----------------------------------------------------------------------------