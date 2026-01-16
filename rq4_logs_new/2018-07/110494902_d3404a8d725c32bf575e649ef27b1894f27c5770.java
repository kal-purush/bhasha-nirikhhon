package org.snobot.lib.logging;

public class CsvLogEntry
{
    private final String mColumnName;
    private String mValue;

    public CsvLogEntry(String aColumnName)
    {
        mColumnName = aColumnName;
        mValue = "UNSET";
    }

    public String getName()
    {
        return mColumnName;
    }

    public void update(int aValue)
    {
        update(Integer.toString(aValue));
    }

    public void update(double aValue)
    {
        update(Double.toString(aValue));
    }

    public void update(boolean aValue)
    {
        update(aValue ? "1" : "0");
    }

    public void update(String aValue)
    {
        mValue = aValue;
    }

    /**
     * Gets the last value sent to the entry, then clears it out.
     * 
     * @return The last value logged
     */
    public String getLastValue()
    {
        String output = mValue;
        mValue = "UNSET";
        return output;
    }

}