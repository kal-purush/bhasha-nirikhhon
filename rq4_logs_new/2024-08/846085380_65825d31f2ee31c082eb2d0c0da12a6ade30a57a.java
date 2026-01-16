package vn.hust.hedspi.ezsport.services.locationidentity;

import com.uber.h3core.H3Core;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service("H3")
public class H3 implements LocationIdentity{
    final int RES = 9;

    @Override
    public String getLocationIdentity(double longitude, double latitude) throws IOException {
        H3Core h3 = H3Core.newSystemInstance();

        return h3.latLngToCellAddress(longitude, latitude, RES);
    }

    public static void main(String[] args) throws IOException {
        H3 block = new H3();
        System.out.println(block.getLocationIdentity(90,45));
    }
}