/*
 * The MIT License
 *
 * Copyright 2015 andrewnyhus.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package java2dscrollinguniverse.Model.actors;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.Shape;
import java2dscrollinguniverse.Model.universe.Universe;

/**
 *
 * @author andrewnyhus
 */
public class HUDMap extends Actor{
    
    private WindowCorner cornerOfWindow;
    private Dimension windowDimension;
    private Universe universe;
    private Actor[] childActors;
    private Shape shape;
    
    private final int inset = 20;
    private final Color colorOfUniverseRepresentation = Color.BLACK;
    private final Color colorOfWindowRepresentation = new Color(210, 210, 210);//gray
    private final Color colorOfOtherChildActors = new Color(227, 148, 0);//orange
    
    public HUDMap(Dimension windowDimension, Universe universe){
        super(ActorType.HUDElement, new Point(0, 0), Color.BLACK);
        //^^ that call to super disregarded colorOfUniverseRepresentation because super must be the first
        //call in the constructor.  That is why the wrong location or point was entered at first as
        // 0, 0  It is simply using the actor class to setup the object so that we can tinker with
        //the data members afterwards
        
        this.windowDimension = windowDimension;
        this.universe = universe;
        this.cornerOfWindow = WindowCorner.BOTTOM_LEFT;
    }
        
    public HUDMap(Dimension windowDimension, Universe universe, WindowCorner cornerOfWindow){
        super(ActorType.HUDElement, new Point(0, 0), Color.BLACK);
        //^^same description/explanation as other constructor's call to super
        this.windowDimension = windowDimension;
        this.universe = universe;
        this.cornerOfWindow = cornerOfWindow;
    }

    
    
    private Actor generateScaledViewRectForMap(){
    
        
        
        return null;
    }
    

    private void generateScaledUniverseRectForMap(){
        int maxWidthMap = (int)(0.3*this.windowDimension.getWidth());
        int maxHeightMap = (int)(0.3*this.windowDimension.getHeight());
        
        boolean heightIsDominantInView = (maxHeightMap > maxWidthMap);
        
        Dimension universeRect;
        int universeRectWidth, universeRectHeight;
        
        if(heightIsDominantInView){
            universeRectWidth = maxWidthMap;
            int ratio = (this.universe.getBoundsDimension().width/universeRectWidth);
            universeRectHeight = this.universe.getBoundsDimension().height/ratio;
        }else{
            universeRectHeight = maxHeightMap;
            int ratio = (this.universe.getBoundsDimension().height/universeRectHeight);
            universeRectWidth = this.universe.getBoundsDimension().width/ratio;
        }
        
        universeRect = new Dimension(universeRectWidth, universeRectHeight);
        
        this.shape = new Rectangle(0, 0, universeRect.width, universeRect.height);
        
        int pointX, pointY;
        
        switch(this.getCornerOfWindow()){
        
            case TOP_RIGHT:
                
                break;
                
            case TOP_LEFT:
                break;
                
            case BOTTOM_LEFT:
                break;
                
            case BOTTOM_RIGHT:
                break;
        
        }
        
    }

    @Override
    public Shape getShape(){
        this.generateScaledUniverseRectForMap();
        return this.shape;
    }
    
    @Override
    public void setShape(Shape s){
        this.shape = s;
    }
    
    /**
     * @return the cornerOfWindow
     */
    public WindowCorner getCornerOfWindow() {
        return cornerOfWindow;
    }

    /**
     * @param cornerOfWindow the cornerOfWindow to set
     */
    public void setCornerOfWindow(WindowCorner cornerOfWindow) {
        this.cornerOfWindow = cornerOfWindow;
    }

    /**
     * @return the windowDimension
     */
    public Dimension getWindowDimension() {
        return windowDimension;
    }

    /**
     * @param windowDimension the windowDimension to set
     */
    public void setWindowDimension(Dimension windowDimension) {
        this.windowDimension = windowDimension;
    }

    /**
     * @return the universe
     */
    private Universe getUniverse() {
        return universe;
    }

    /**
     * @param universe the universe to set
     */
    public void setUniverse(Universe universe) {
        this.universe = universe;
    }

    /**
     * @param childActors the childActors to set
     */
    @Override
    public void setChildActors(Actor[] childActors) {
        this.childActors = childActors;
    }

    /**
     * @return the childActors
     */
    @Override
    public Actor[] getChildActors() {
        return childActors;
    }
    
    
    
    
    
    
    public enum WindowCorner{
        TOP_RIGHT(0), TOP_LEFT(1), BOTTOM_LEFT(2), BOTTOM_RIGHT(3);
        
        private int value;
        
        private WindowCorner(int value){
            this.value = value;
        }
        
        public int getValue(){
            return this.value;
        }
    }
    
}