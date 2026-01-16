package org.scorpio.microscope;

import org.scorpio.microscope.enginex.common.MLogApi;
import org.scorpio.microscope.enginex.common.MLogQuery;
import org.scorpio.microscope.enginex.common.MLogSystemOutImpl;
import org.scorpio.microscope.enginex.core.MDriverCore;

public class MCmd extends MDriverCore {
    private static MLogApi log = new MLogSystemOutImpl();

    static {
        MLogQuery.addP6Log(log);
        MDriverCore.init();
    }
}
