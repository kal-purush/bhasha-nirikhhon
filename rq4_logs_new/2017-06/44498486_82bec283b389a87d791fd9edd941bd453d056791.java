/**
 * MIT License
 *
 * Copyright (c) 2017 grndctl
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.grndctl;

import com.grndctl.services.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 */
public abstract class SvcTestSupport extends TestProperties {

    protected static final Logger LOG = LogManager.getLogger(SvcTestSupport.class);

    protected final AircraftReportSvc aircraftReportSvc;

    protected final AirlineSvc airlineSvc;

    protected final AirsigmetSvc airsigmetSvc;

    protected final ChartsSvc chartsSvc;

    protected final MetarSvc metarSvc;

    protected final NavaidSvc navaidSvc;

    protected final NotamSvc notamSvc;

    protected final PirepSvc pirepSvc;

    protected final StationSvc stationSvc;

    protected final TafSvc tafSvc;

    protected SvcTestSupport() {
        this.aircraftReportSvc = new AircraftReportSvc();
        this.airlineSvc = new AirlineSvc();
        this.airsigmetSvc = new AirsigmetSvc();
        this.chartsSvc = new ChartsSvc();
        this.metarSvc = new MetarSvc();
        this.navaidSvc = new NavaidSvc();
        this.notamSvc = new NotamSvc();
        this.pirepSvc = new PirepSvc();
        this.stationSvc = new StationSvc();
        this.tafSvc = new TafSvc();
    }

}