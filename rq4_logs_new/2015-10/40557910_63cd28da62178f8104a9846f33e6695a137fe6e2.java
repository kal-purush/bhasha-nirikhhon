package com.hitchh1k3rsguide.$CORE_REPLACE$.hitchcore;

public class VersionInfo implements Comparable
{

    public final int    verInt;
    public final String verStr;
    public final int    devBuild;

    public VersionInfo(String versionString)
    {
        String[] d = versionString.split("a");
        String[] v = d[0].split("\\.");
        String verBuilder = d[0];
        if (d.length > 1)
        {
            devBuild = Integer.parseInt(d[1]);
            verBuilder += " Pre" + devBuild;
        }
        else
        {
            devBuild = 0;
        }
        verStr = verBuilder;
        int[] vi = new int[3];
        for (int i = 0; i < v.length; ++i)
        {
            vi[i] = Integer.parseInt(v[i]);
        }
        verInt = (vi[0] * 10000) + (vi[1] * 100) + (vi[2]);
    }

    @Override
    public boolean equals(Object rhs)
    {
        return ((rhs instanceof VersionInfo) && verStr.equals(((VersionInfo) rhs).verStr));
    }

    @Override
    public int compareTo(Object o)
    {
        if (o instanceof VersionInfo)
        {
            int v = verInt - ((VersionInfo) o).verInt;
            if (v == 0 && devBuild != 0 && ((VersionInfo) o).devBuild != 0)
            {
                return devBuild - ((VersionInfo) o).devBuild;
            }
            return v;
        }
        return 0;
    }
}