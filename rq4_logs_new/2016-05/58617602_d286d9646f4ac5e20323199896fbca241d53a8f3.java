
import java.util.Scanner;
import java.io.File;
import java.util.ArrayList;
import com.sun.j3d.utils.universe.SimpleUniverse;
import com.sun.j3d.utils.geometry.GeometryInfo;
import com.sun.j3d.utils.geometry.NormalGenerator;
import com.sun.j3d.utils.behaviors.mouse.MouseRotate;
import com.sun.j3d.utils.behaviors.mouse.MouseTranslate;
import com.sun.j3d.utils.behaviors.mouse.MouseWheelZoom;
import com.sun.j3d.utils.geometry.Text2D;
import com.sun.j3d.utils.universe.Viewer;
import com.sun.j3d.utils.universe.ViewingPlatform;
import javax.media.j3d.Canvas3D;
import javax.media.j3d.BranchGroup;
import javax.media.j3d.TransformGroup;
import javax.media.j3d.AmbientLight;
import javax.media.j3d.PointLight;
import javax.media.j3d.BoundingSphere;
import javax.media.j3d.Appearance;
import javax.media.j3d.Material;
import javax.media.j3d.PolygonAttributes;
import javax.media.j3d.Shape3D;
import javax.media.j3d.TriangleArray;
import javax.media.j3d.GeometryArray;
import javax.vecmath.Point3d;
import javax.vecmath.Color3f;
import java.awt.Color;
import java.awt.Font;
import javax.media.j3d.PhysicalBody;
import javax.media.j3d.PhysicalEnvironment;
import javax.media.j3d.Transform3D;
import javax.media.j3d.View;
import javax.vecmath.Vector3d;

public class Off extends SimpleUniverse {

    public final int POLYGON_POINT = javax.media.j3d.PolygonAttributes.POLYGON_POINT;
    public final int POLYGON_LINE = javax.media.j3d.PolygonAttributes.POLYGON_LINE;
    public final int POLYGON_FILL = javax.media.j3d.PolygonAttributes.POLYGON_FILL;
    public final int GROUP_BG = 0;
    public final int LIGHT_BG = 1;
    public final int SHAPE_TG = 0;
    private Color3f color;//Manter a cor atual

    private int npoints;
    private double maxdist;
    private Shape3D shape;
    private BranchGroup bg;
    private ArrayList<Point3d> points;

    MouseWheelZoom mwz;
    MouseRotate mr;
    MouseTranslate mt;

    public Off(Canvas3D cv) {
        super(cv);

        bg = new BranchGroup();
        bg.setCapability(BranchGroup.ALLOW_DETACH);
        TransformGroup tg = new TransformGroup();
        tg.setCapability(TransformGroup.ALLOW_TRANSFORM_READ);
        tg.setCapability(TransformGroup.ALLOW_TRANSFORM_WRITE);

        bg.insertChild(tg, GROUP_BG);

        mwz = new MouseWheelZoom();
        mr = new MouseRotate();
        mt = new MouseTranslate();
        mwz.setFactor(mwz.getFactor() * -1);
        mwz.setTransformGroup(tg);
        mr.setTransformGroup(tg);
        mt.setTransformGroup(tg);
        mwz.setSchedulingBounds(tg.getBounds());
        mr.setSchedulingBounds(tg.getBounds());
        mt.setSchedulingBounds(tg.getBounds());

        tg.addChild(mwz);
        tg.addChild(mr);
        tg.addChild(mt);

        bg.insertChild(new BranchGroup(), LIGHT_BG);
    }

    public Off(ViewingPlatform vp, Viewer vw) {
        super(vp, vw);

        bg = new BranchGroup();
        bg.setCapability(BranchGroup.ALLOW_DETACH);
        TransformGroup tg = new TransformGroup();
        tg.setCapability(TransformGroup.ALLOW_TRANSFORM_READ);
        tg.setCapability(TransformGroup.ALLOW_TRANSFORM_WRITE);

        bg.insertChild(tg, GROUP_BG);

        mwz = new MouseWheelZoom();
        mr = new MouseRotate();
        mt = new MouseTranslate();
        mwz.setFactor(mwz.getFactor() * -1);
        mwz.setTransformGroup(tg);
        mr.setTransformGroup(tg);
        mt.setTransformGroup(tg);
        mwz.setSchedulingBounds(tg.getBounds());
        mr.setSchedulingBounds(tg.getBounds());
        mt.setSchedulingBounds(tg.getBounds());

        Text2D marks = new Text2D("hbqvirvndqmpvruvhn0idn", new Color3f(Color.YELLOW), "Helvetica", 12, Font.PLAIN);
        marks.setString("hjb9ygvu");
        bg.addChild(marks);
        //bg.add(marks);
        //
        addlight();
        //bg.insertChild(new BranchGroup(), LIGHT_BG);
//        System.out.println(bg.getBounds());
        this.addBranchGraph(bg);
//        System.out.println(marks);
    }

    public void OpenFile(File file) {
        bg.detach();
        TransformGroup tg = ((TransformGroup) bg.getChild(GROUP_BG));
        tg.removeAllChildren();

        shape = loadoff(file);
        tg.insertChild(shape, SHAPE_TG);

        mwz.setTransformGroup(tg);
        tg.addChild(mwz);
        mwz.setSchedulingBounds(tg.getBounds());
        mr.setTransformGroup(tg);
        tg.addChild(mr);
        mr.setSchedulingBounds(tg.getBounds());
        mt.setTransformGroup(tg);
        tg.addChild(mt);
        mt.setSchedulingBounds(tg.getBounds());

        TransformGroup tt = new TransformGroup();
//        Text2D marks = new Text2D("hey", new Color3f(Color.YELLOW),
//                "Helvetica", 80, Font.PLAIN);
//        tg.addChild(marks);
//        Transform3D t = new Transform3D();
//        t.setTranslation(null);
///        tt.setTransform(null);

        addlight();
        this.addBranchGraph(bg);
        this.getViewingPlatform().setNominalViewingTransform();
//        Transform3D tf = new Transform3D();
//        tf.setTranslation(new Vector3d(0, 0, -maxdist*(Math.sqrt(maxdist+1))));
//        tg.setTransform(tf);
        Transform3D tf = new Transform3D();
        tf.setScale(1 / maxdist);
        tg.setTransform(tf);
//        
        this.getViewer().getView().setBackClipDistance(500);
        this.getViewer().getView().setFrontClipDistance(0.01);
        this.getViewer().getView().setFieldOfView(Math.toRadians(90));
        System.out.println(maxdist);
    }

    private Shape3D loadoff(File file) {
        maxdist = 0;
        try {
            Scanner sc = new Scanner(file).useLocale(java.util.Locale.US);
            sc.nextLine();

            int np = sc.nextInt();
            int nf = sc.nextInt();
            int ne = sc.nextInt();
            npoints = np;

            points = new ArrayList<>();
            TriangleArray ta = new TriangleArray(nf * 3, TriangleArray.COORDINATES | GeometryArray.COLOR_3);
            Color3f[] cl = new Color3f[nf * 3];

            for (int x = 0; x < np; x++) {
                points.add(new Point3d(sc.nextFloat(), sc.nextFloat(), sc.nextFloat()));
                if (Math.abs(points.get(x).x) > maxdist) {
                    maxdist = Math.abs(points.get(x).x);
                }
                if (Math.abs(points.get(x).y) > maxdist) {
                    maxdist = Math.abs(points.get(x).y);
                }
                if (Math.abs(points.get(x).z) > maxdist) {
                    maxdist = Math.abs(points.get(x).z);
                }

//                if(points.get(x).x > maxdist)maxdist = points.get(x).x;
//                if(points.get(x).y > maxdist)maxdist = points.get(x).y;
//                if(points.get(x).z > maxdist)maxdist = points.get(x).z;
            }

            maxdist *= 2;

            for (int x = 0; x < np; x++) {
                Point3d p = points.remove(x);
                p.setX(p.getX() / maxdist);
                p.setY(p.getY() / maxdist);
                p.setZ(p.getZ() / maxdist);
                points.add(x, p);
            }
            Printer.print(points, "C:\\Users\\matheus\\Desktop\\cowar.txt");
            for (int x = 0; x < nf * 3;) {
                for (int nsides = sc.nextInt(); nsides > 0; nsides--) {
                    Point3d co = points.get(sc.nextInt());
//                    cl[x] = new Color3f(Color.green);
                    /**
                     * Colorir*
                     */
                    if (co.x > 0) {
                        cl[x] = new Color3f(Color.CYAN);
                    } else {
                        cl[x] = new Color3f(Color.ORANGE);
                    }

                    ta.setCoordinate(x++, co);
                }
            }
            ta.setColors(0, cl);//Colorir
            //Printer.print(ta, "C:\\Users\\matheus\\Desktop\\cowta.txt");
            return makeShape(ta);
        } catch (Exception e) {
            System.out.println("Erro ao tentar contruir OFF\n" + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    private Shape3D makeShape(TriangleArray tarray) {
        GeometryInfo forminfo = new GeometryInfo(tarray);
        NormalGenerator ng = new NormalGenerator();
        ng.generateNormals(forminfo);

        Appearance a = new Appearance();
        a.setMaterial(new Material());
        a.setPolygonAttributes(new PolygonAttributes());

        return new Shape3D(forminfo.getGeometryArray(), a);
    }

    public void setCollor(Color cl) {
        bg.detach();
        Appearance a = shape.getAppearance();
        Material mat = a.getMaterial();
        mat.setDiffuseColor(new Color3f(cl));
        a.setMaterial(mat);

        shape.setAppearance(a);
        this.addBranchGraph(bg);
    }

    public void setPAttributes(int i) {
        bg.detach();
        Appearance a = shape.getAppearance();
        PolygonAttributes pa = a.getPolygonAttributes();
        pa.setPolygonMode(i);
        a.setPolygonAttributes(pa);

        shape.setAppearance(a);
        this.addBranchGraph(bg);
    }

    public void selectPoint(Point3d ps) {
        TriangleArray ta = (TriangleArray) shape.getGeometry();
        TransformGroup tg = (TransformGroup) bg.getChild(GROUP_BG);
        //tg.removeChild(SHAPE_TG);
        Point3d pi = new Point3d();
        Color3f[] cl = new Color3f[ta.getVertexCount()];
        double min = Double.MAX_VALUE;
        int s = 0;
        double dist;
        for (int i = 0; i < ta.getVertexCount(); i++) {
            ta.getCoordinate(i, pi);
            dist = dist(pi, ps);
            if (dist < min) {
                min = dist;
                s = i;
            }
//            if(pi == ps){
//                //System.out.println(pi +" == " + ps);
//                cl[i] = new Color3f(Color.CYAN);
//            }else{
//                //System.out.println(pi +" != " + ps);
//                cl[i] = new Color3f(Color.DARK_GRAY);
//            }
        }
        PolygonAttributes pa = shape.getAppearance().getPolygonAttributes();
        bg.detach();
        ta.setCapability(TriangleArray.COORDINATES | GeometryArray.COLOR_3);
        ta.setCapability(GeometryArray.COLOR_3);
        ta.setColor(s, new Color3f(Color.CYAN));
        shape = makeShape(ta);
        shape.getAppearance().setPolygonAttributes(pa);
        tg.removeChild(shape);
        tg.insertChild(shape, SHAPE_TG);
        this.addBranchGraph(bg);
    }

    private double dist(Point3d p1, Point3d p2) {
        return Math.sqrt(
                Math.pow(p1.x - p2.x, 2)
                + Math.pow(p1.y - p2.y, 2)
                + Math.pow(p1.z - p2.z, 2)
        );
    }

    private void addlight() {
        bg.removeChild(LIGHT_BG);
        BranchGroup light = new BranchGroup();
        light.setBounds(new BoundingSphere(new Point3d(0, 0, 0), maxdist * 3));
        AmbientLight la = new AmbientLight(new Color3f(0.1f, 0.1f, 0.1f));
        la.setInfluencingBounds(new BoundingSphere(new Point3d(0, 0, 0), maxdist * 3));
        light.addChild(la);

        PointLight pl = new PointLight();
        pl.setColor(new Color3f(Color.WHITE));
        pl.setPosition(0, 0, 2);
        pl.setInfluencingBounds(new BoundingSphere(new Point3d(0, 0, 0), maxdist * 3));
        light.addChild(pl);
        bg.insertChild(light, LIGHT_BG);
    }

    public int getnPoints() {
        return npoints;
    }

    public int getnFaces() {
        TriangleArray ta = (TriangleArray) shape.getGeometry();
        return (ta.getVertexCount() / 3);
    }

    public TransformGroup getTransformGroup() {
        return (TransformGroup) bg.getChild(GROUP_BG);
    }

    public BranchGroup getBranchGroup() {
        return bg;
    }

    public void print() {
        System.out.println("hjfkfdufl\nljduf");
    }

    //ViewingPlatform vp = this.getViewingPlatform().getViewPlatform().set
    //this.getViewingPlatform().setCapability(ViewingPlatform.ALLOW_AUTO_COMPUTE_BOUNDS_WRITE);
    //this.getViewingPlatform().setBoundsAutoCompute(true);
    //this.getViewingPlatform().compile();
}