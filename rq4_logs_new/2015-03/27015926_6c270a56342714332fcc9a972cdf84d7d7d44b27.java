/******************************************************************************
 * Title: MutableDouble.java
 * Author: Hunter Schoonover
 * Date: 09/26/14
 *
 * Purpose:
 *
 * This class is a mutable double wrapper.
 *
 */

//-----------------------------------------------------------------------------

package com.yaboosh.ybtech.lasertally;

//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
//-----------------------------------------------------------------------------
// class MutableDouble
//

public class MutableDouble {

    double value;

    //-----------------------------------------------------------------------------
    // MutableDouble::MutableDouble (constructor)
    //

    public MutableDouble(double pValue)
    {

        value = pValue;

    }//end of MutableDouble::MutableDouble (constructor)
    //-----------------------------------------------------------------------------

    //-----------------------------------------------------------------------------
    // MutableDouble::isEqualTo
    //

    public boolean isEqualTo(double pValue)
    {

        boolean equal = false;

        if (pValue == value) { equal = true; }

        return equal;

    }//end of MutableDouble::isEqualTo
    //-----------------------------------------------------------------------------

    //-----------------------------------------------------------------------------
    // MutableDouble::getValue
    //

    public double getValue()
    {

        return value;

    }//end of MutableDouble::getValue
    //-----------------------------------------------------------------------------

    //-----------------------------------------------------------------------------
    // MutableDouble::setValue
    //

    public void setValue(double pValue)
    {

        value = pValue;

    }//end of MutableDouble::setValue
    //-----------------------------------------------------------------------------

}//end of class MutableDouble
//-----------------------------------------------------------------------------
//-----------------------------------------------------------------------------