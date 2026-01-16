package mouse;

import java.awt.Point;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import resize.IResize;

/**
 *
 * @author ΙΩΑΝΝΑ
 */
public class MouseAdapterForMoving extends MouseAdapter{
    
    private final IResize iResizeHandle;
    private Point2D oldP;
    
    public MouseAdapterForMoving(IResize iResizeHandle){
        this.iResizeHandle = iResizeHandle;
    }
    
    @Override
    public void mousePressed(MouseEvent event) {
    
       Point p = event.getPoint();
       oldP = event.getPoint();//blepw pou einai to pontiki->Pressed
       Rectangle2D r = new Rectangle2D.Double();
       
       for (int i = 0; i < iResizeHandle.getPoints().length; i = i + 2) {
            //edw entopizei pou briskete ta mikra rectangle
            r.setFrameFromDiagonal(iResizeHandle.getPoints()[i], 
                    iResizeHandle.getPoints()[i+1]);
            if (r.contains(p)) {

                iResizeHandle.setPosForItem(i);//TODO
                return;
            }
        }
    }
    

    @Override
    public void mouseReleased(MouseEvent event) {
        iResizeHandle.setPosForItem(-1);
    }

    @Override
    public void mouseDragged(MouseEvent event) {

        if (iResizeHandle.getPosForItem()== -1) {
            return;
        }
        
        Point2D p3 = new Point();
        
        p3.setLocation(event.getPoint().getX() - oldP.getX() ,event.getPoint().getY() - oldP.getY() );
        oldP = event.getPoint();//auto einai to kainourgio Pressed
        
        Point2D pToGo = new Point();
        pToGo.setLocation(
                iResizeHandle.getPoints()[iResizeHandle.getPosForItem()].getX() 
                        + p3.getX(),
                iResizeHandle.getPoints()[iResizeHandle.getPosForItem()].getY() 
                        + p3.getY());
        iResizeHandle.getPoints()[iResizeHandle.getPosForItem()] = new Point((Point)pToGo);
        
        pToGo.setLocation(
                iResizeHandle.getPoints()[iResizeHandle.getPosForItem() + 1].getX() 
                        + p3.getX(),
                iResizeHandle.getPoints()[iResizeHandle.getPosForItem() + 1].getY() 
                        + p3.getY());
        iResizeHandle.getPoints()[iResizeHandle.getPosForItem() + 1] = new Point((Point)pToGo);
        
        iResizeHandle.doUpdate();
    }
}