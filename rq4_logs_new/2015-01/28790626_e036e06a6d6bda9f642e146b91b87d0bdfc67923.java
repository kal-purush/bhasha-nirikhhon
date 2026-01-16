package ch.n1b.bitfield;

import ch.n1b.vector.Vec3D;
import org.junit.Test;

/**
 * Created on 07.01.2015.
 *
 * @author Thomas
 */
public class Vec3DTest {

    @Test
    public void crossTest(){
        Vec3D e1 = new Vec3D(2,0,0);
        Vec3D e2 = new Vec3D(0,2,0);
        Vec3D cross = e1.cross(e2);
        System.out.println(cross);
        System.out.println(cross.binary());
        System.out.println(cross.norm2());
    }
}