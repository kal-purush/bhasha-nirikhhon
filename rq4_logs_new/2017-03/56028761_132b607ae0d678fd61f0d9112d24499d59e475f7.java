package whiteSocket;

public class Stretch {

	public static int ax,ay,bx,by,cx,cy,dx,dy,fx,fy,gx,hy;
	public static double ex, ey, gy, hx = 0;
	public static double mA, mB, mC, mD, yCenterIN, xCenterIN, xCenterOUT, yCenterOUT, xOUT_temp,
						yOUT_temp, lxA, lxB, lyA, lyB, kxA, kxB, kyA, kyB, jx, jy, ix, iy, lx, ly,
						kx, ky, A, B, C, lA, lB, lC, lD, lE, lF, lG, lH;


	/**
	 * STRETCH() method: input pixel location -> output pixel location
	 *
	 *	This method is the core of Blooprint.xyz its input-output mechanism should remain as-is.
	 *	Please see derivation approach in project description.
	 * */
	public static int[] stretch(int x, int y) {


		int[] some = new int[2];

		jx = ((double)y - ((double)x * mB) + (xCenterIN * mA) - yCenterIN) / (mA - mB);
        jy = (mA * (jx - xCenterIN)) + yCenterIN;
        ix = ((double)y - ((double)x * mA) + (xCenterIN * mB) - yCenterIN) / (mB - mA);
        iy = (mB * (ix - xCenterIN)) + yCenterIN;

//        System.out.println("============================STRETCH==================================");
//        System.out.println("jx = "+jx+"\tjy = "+jy);
//        System.out.println("ix = "+ix+"\tiy = "+iy);


        if (jy >= yCenterIN)
        {
            lA = Math.sqrt((Math.pow(jx - xCenterIN, 2)) + (Math.pow(jy - yCenterIN, 2)));
            lB = Math.sqrt((Math.pow(bx - xCenterIN, 2)) + (Math.pow(by - yCenterIN, 2)));
            lF = Math.sqrt((Math.pow(fx - xCenterOUT, 2)) + (Math.pow(fy - yCenterOUT, 2)));

            lE = lA * lF / lB;

//            System.out.println("lA = "+lA+"\tlB = "+lB+"\tlF = "+lF+"\tlE = "+lE);

            A = 1 + Math.pow(mC, 2);
            B = (-2 * xCenterOUT) - (2 * fx * Math.pow(mC, 2)) + (2 * fy * mC) - (2 * yCenterOUT * mC);
            C = Math.pow(xCenterOUT, 2) + Math.pow(fx * mC, 2) - (2 * fx * fy * mC) + Math.pow(fy, 2) + (2 * yCenterOUT * fx * mC) - (2 * yCenterOUT * fy) + Math.pow(yCenterOUT, 2) - Math.pow(lE, 2);

            lxA = (-B + Math.sqrt(Math.pow(B, 2) - (4 * A * C))) / (2 * A);
            lxB = (-B - Math.sqrt(Math.pow(B, 2) - (4 * A * C))) / (2 * A);

            lyA = (mC * (lxA - fx)) + fy;
            lyB = (mC * (lxB - fx)) + fy;

            if (lyA >= yCenterOUT)
            {
                lx = lxA;
                ly = lyA;
            }
            else
            {
                lx = lxB;
                ly = lyB;
            }
        }
        else
        {
            lA = Math.sqrt((Math.pow(jx - xCenterIN, 2)) + (Math.pow(jy - yCenterIN, 2)));
            lB = Math.sqrt((Math.pow(ax - xCenterIN, 2)) + (Math.pow(ay - yCenterIN, 2)));
            lF = Math.sqrt((Math.pow(ex - xCenterOUT, 2)) + (Math.pow(ey - yCenterOUT, 2)));

            lE = lA * lF / lB;

//            System.out.println("lA = "+lA+"\tlB = "+lB+"\tlF = "+lF+"\tlE = "+lE);


            A = 1 + Math.pow(mC, 2);
            B = (-2 * xCenterOUT) - (2 * ex * Math.pow(mC, 2)) + (2 * ey * mC) - (2 * yCenterOUT * mC);
            C = Math.pow(xCenterOUT, 2) + Math.pow(ex * mC, 2) - (2 * ex * ey * mC) + Math.pow(ey, 2) + (2 * yCenterOUT * ex * mC) - (2 * yCenterOUT * ey) + Math.pow(yCenterOUT, 2) - Math.pow(lE, 2);

            lxA = (-B + Math.sqrt(Math.pow(B, 2) - (4 * A * C))) / (2 * A);
            lxB = (-B - Math.sqrt(Math.pow(B, 2) - (4 * A * C))) / (2 * A);

            lyA = (mC * (lxA - ex)) + ey;
            lyB = (mC * (lxB - ex)) + ey;

            if (lyA < yCenterOUT)
            {
                lx = lxA;
                ly = lyA;
            }
            else
            {
                lx = lxB;
                ly = lyB;
            }
        }

        if (iy >= yCenterIN)
        {
            lC = Math.sqrt((Math.pow(ix - xCenterIN, 2)) + (Math.pow(iy - yCenterIN, 2)));
            lD = Math.sqrt((Math.pow(dx - xCenterIN, 2)) + (Math.pow(dy - yCenterIN, 2)));
            lH = Math.sqrt((Math.pow(hx - xCenterOUT, 2)) + (Math.pow(hy - yCenterOUT, 2)));

            lG = lC * lH / lD;

//            System.out.println("lC = "+lC+"\tlD = "+lD+"\tlH = "+lH+"\tlG = "+lG);


            A = 1 + Math.pow(mD, 2);
            B = (-2 * xCenterOUT) - (2 * hx * Math.pow(mD, 2)) + (2 * hy * mD) - (2 * yCenterOUT * mD);
            C = Math.pow(xCenterOUT, 2) + Math.pow(hx * mD, 2) - (2 * hx * hy * mD) + Math.pow(hy, 2) + (2 * yCenterOUT * hx * mD) - (2 * yCenterOUT * hy) + Math.pow(yCenterOUT, 2) - Math.pow(lG, 2);

            kxA = (-B + Math.sqrt(Math.pow(B, 2) - (4 * A * C))) / (2 * A);
            kxB = (-B - Math.sqrt(Math.pow(B, 2) - (4 * A * C))) / (2 * A);

            kyA = (mD * (kxA - hx)) + hy;
            kyB = (mD * (kxB - hx)) + hy;

            if (kyA >= yCenterOUT)
            {
                kx = kxA;
                ky = kyA;
            }
            else
            {
                kx = kxB;
                ky = kyB;
            }
        }
        else
        {
            lC = Math.sqrt((Math.pow(ix - xCenterIN, 2)) + (Math.pow(iy - yCenterIN, 2)));
            lD = Math.sqrt((Math.pow(cx - xCenterIN, 2)) + (Math.pow(cy - yCenterIN, 2)));
            lH = Math.sqrt((Math.pow(gx - xCenterOUT, 2)) + (Math.pow(gy - yCenterOUT, 2)));

            lG = lC * lH / lD;

//            System.out.println("lC = "+lC+"\tlD = "+lD+"\tlH = "+lH+"\tlG = "+lG);


            A = 1 + Math.pow(mD, 2);
            B = (-2 * xCenterOUT) - (2 * gx * Math.pow(mD, 2)) + (2 * gy * mD) - (2 * yCenterOUT * mD);
            C = Math.pow(xCenterOUT, 2) + Math.pow(gx * mD, 2) - (2 * gx * gy * mD) + Math.pow(gy, 2) + (2 * yCenterOUT * gx * mD) - (2 * yCenterOUT * gy) + Math.pow(yCenterOUT, 2) - Math.pow(lG, 2);

//            System.out.println("lC = "+lC+"\tlD = "+lD+"\tlH = "+lH+"\tlG = "+lG);


            kxA = (-B + Math.sqrt(Math.pow(B, 2) - (4 * A * C))) / (2 * A);
            kxB = (-B - Math.sqrt(Math.pow(B, 2) - (4 * A * C))) / (2 * A);

            kyA = (mD * (kxA - gx)) + gy;
            kyB = (mD * (kxB - gx)) + gy;

            if (kyA < yCenterOUT)
            {
                kx = kxA;
                ky = kyA;
            }
            else
            {
                kx = kxB;
                ky = kyB;
            }
        }


        xOUT_temp = (ky - ly + (lx * mD) - (kx * mC)) / (mD - mC);
        yOUT_temp = (mD * (xOUT_temp - lx)) + ly;


        some[0] = (int) Math.round(xOUT_temp);
        some[1] = (int) Math.round(yOUT_temp);


		return some;
	}//END stretch()

}