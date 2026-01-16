package org.monarchinitiative.vmvt.core.svg.fontprofile;

/**
 * Courier is a classic monospace font that is available on most systems
 * @author Peter N Robinson
 */
public class CourierProfile extends FontProfile {
    /** This is a magic number that places the letters in the correct vertical position. Works with {@link #LOGO_COLUMN_HEIGHT}.*/
    protected final double VERTICAL_SCALING_FACTOR = 1.45;//1.14;
    /** Maximum height of the letters in the sequence logo. Needs to be adjusted together with {@link #VERTICAL_SCALING_FACTOR}.*/
    protected final double LOGO_COLUMN_HEIGHT = 20.0;
    /**  and need a factor to calculate the proper height of the grey box surroundiing variant positions. */
    private final int VARIANT_BOX_SCALING_FACTOR = 12;
    @Override
    public double verticalScalingFactor() {
        return VERTICAL_SCALING_FACTOR;
    }
    @Override
    public double logoColumnHeight() {
        return LOGO_COLUMN_HEIGHT;
    }

    @Override
    public String fonts() {
        return "courier, monospace";
    }

    @Override
    public int variantBoxScalingFactor() {
        return VARIANT_BOX_SCALING_FACTOR;
    }

}