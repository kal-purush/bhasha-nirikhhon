package ch.so.agi.landuseplansextract.webservice.services;

import ch.admin.geo.schemas.v_d.oereb._1_0.extract.GetEGRIDResponseType;

public interface GetEGRIDResponseTypeService {
    GetEGRIDResponseType getGetEGRIDResponseTypeByXY(double easting, double northing);
}