package local.deva.kmldroid.model.Geometry;

/**
 * Created by Ram on 4/1/2015.
 */
import local.deva.kmldroid.model.Scale;
import local.deva.kmldroid.model.atom.Link;
import local.deva.kmldroid.model.enums.AltitudeMode;
import local.deva.kmldroid.model.Location;
import local.deva.kmldroid.model.Orientation;
import lombok.Getter;
import lombok.Setter;
@Getter
@Setter
public class Model extends Geometry {
    private AltitudeMode altitudeMode;
    private Location location;
    private Orientation orientation;
    private Link link;
    private ResourceMap resourceMap;
    private Scale scale;

}