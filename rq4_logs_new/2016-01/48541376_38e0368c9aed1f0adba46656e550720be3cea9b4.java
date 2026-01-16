/*
 * The MIT License
 *
 * Copyright 2016 andrewnyhus.
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
package java2dscrollinguniverse.View;

import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GridLayout;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.geom.Ellipse2D;
import java2dscrollinguniverse.Model.actors.Actor;
import java2dscrollinguniverse.Model.actors.ActorLabel;
import java2dscrollinguniverse.Model.container.ContainerUniverse;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JColorChooser;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

/**
 *
 * @author andrewnyhus
 */
public class EditActorPane extends JOptionPane{
    private final ContainerUniverse containerUniverse;
    private final Actor currentActor;
    private final Actor initialActor;
    
    private String[] arrayOfLabelPositions;
    private JButton plusWidthBtn, plusHeightBtn, minusWidthBtn, minusHeightBtn,
            plusX, plusY, minusX, minusY,
            applyChangesToActorButton, deleteActorButton, changeActorColorButton;
    
    private JLabel widthLbl, heightLbl, xyLabel, invalidChangesWarningLabel;
    private JTextField changeTextField;

    
    public EditActorPane(Actor a, ContainerUniverse containerUniverse){
        super("", JOptionPane.PLAIN_MESSAGE, JOptionPane.DEFAULT_OPTION, null);

        this.applyChangesToActorButton = new JButton("Apply Changes to Actor");
        
        this.applyChangesToActorButton.addActionListener((ActionEvent evt) -> {
        
            EditActorPane.this.updateEditorPane();
            this.applyChanges();
            
            Window w = SwingUtilities.getWindowAncestor(applyChangesToActorButton);

            if (w != null) {
                w.setVisible(false);
            }
        });  
        
        
        this.deleteActorButton = new JButton("Delete Actor");
        
        this.deleteActorButton.addActionListener((ActionEvent evt) -> {
            
            this.deleteActor();
            
            Window w = SwingUtilities.getWindowAncestor(applyChangesToActorButton);

            if (w != null) {
                w.setVisible(false);
            }
        });  
        
                
        
        Object[] userOptions = {this.applyChangesToActorButton, this.deleteActorButton};
        this.setOptions(userOptions);
        
        
        
        this.initialActor = a;
        this.currentActor = Actor.copyInstanceOfActor(a);
        this.containerUniverse = containerUniverse;
        
        
        Object[] message = this.getObjectArrayMessage();
        this.setMessage(message);
        
     

        this.updateEditorPane();
    }
    

    private boolean actorShapeIsRectOrEllipse(){
        Shape s = this.currentActor.getShape();
        return ((s instanceof Rectangle)||(s instanceof Ellipse2D.Double));
    }
    
    private void applyChanges(){
        int indexOfOriginalActor = this.getActorIndexInContainerUniverse();
        
        if(indexOfOriginalActor == -1){
            System.out.println("Changes not applied, index error");
        }else {
            
            this.initialActor.setTopLeftLocation(this.currentActor.getTopLeftLocation());
            this.initialActor.setShape(this.currentActor.getShape());
            this.initialActor.setIdNumber(this.currentActor.getIdNumber());
            this.initialActor.setChildActors(this.currentActor.getChildActors());
            this.initialActor.setActorLabel(new ActorLabel(this.changeTextField.getText(),
                                        this.initialActor.getActorLabel().getPosition()));
            this.initialActor.setColor(this.currentActor.getColor());
            
            this.containerUniverse.getMembersOfContainer().remove(indexOfOriginalActor);
            this.containerUniverse.getMembersOfContainer().add(indexOfOriginalActor, this.initialActor);
        }
    }


    private void deleteActor(){
        int index = this.getActorIndexInContainerUniverse();
        this.containerUniverse.getMembersOfContainer().remove(index);
    }
    
    private int getActorIndexInContainerUniverse(){
        int actorIDNumber = this.initialActor.getIdNumber();
        int i = 0;
        for(Actor a: this.containerUniverse.getMembersOfContainer()){
            if(a.getIdNumber() == actorIDNumber){
                return i;
            }
            i++;
        }
        
        return -1;
        
    }    
    
    private Object[] getObjectArrayMessage(){
        
        this.initComponents();

        String shapeTypeString = "Shape: " + this.currentActor.getShape().getClass().getSimpleName();
        Container widthHeightPane;
        
        
        Container xPlusMinusBtns = new Container();
        xPlusMinusBtns.setLayout(new GridLayout(1, 2));
        
        xPlusMinusBtns.add(this.plusX);
        xPlusMinusBtns.add(this.minusX);
        
        Container yPlusMinusBtns = new Container();
        yPlusMinusBtns.setLayout(new GridLayout(1, 2));
        
        yPlusMinusBtns.add(this.plusY);
        yPlusMinusBtns.add(this.minusY);
        
        Container xyPane = new Container();
        xyPane.setLayout(new GridLayout(1, 2));
        xyPane.add(xPlusMinusBtns);
        xyPane.add(yPlusMinusBtns);
        
        
        
        if(this.actorShapeIsRectOrEllipse()){
            Container plusMinusWidthBtns = new Container();
            plusMinusWidthBtns.setLayout(new GridLayout(1, 2));

            plusMinusWidthBtns.add(this.plusWidthBtn);
            plusMinusWidthBtns.add(this.minusWidthBtn);


            Container plusMinusHeightBtns = new Container();
            plusMinusHeightBtns.setLayout(new GridLayout(1, 2));

            plusMinusHeightBtns.add(this.plusHeightBtn);
            plusMinusHeightBtns.add(this.minusHeightBtn);


            widthHeightPane = new Container();
            widthHeightPane.setLayout(new GridLayout(2, 2));

            widthHeightPane.add(plusMinusWidthBtns);
            widthHeightPane.add(this.widthLbl);

            widthHeightPane.add(plusMinusHeightBtns);
            widthHeightPane.add(this.heightLbl);
            
            Object[] arrayMessage = {
                this.changeActorColorButton,
                shapeTypeString,
                widthHeightPane,
                this.xyLabel, xyPane, 
                "Actor Label:", this.changeTextField,
                this.invalidChangesWarningLabel
            };
            
            return arrayMessage;            
        }else{
            Object[] arrayMessage = {
                this.changeActorColorButton,
                shapeTypeString,
                "This actor is not an ellipse or rectangle, so you can only edit the location:",
                this.xyLabel, xyPane,
                "Actor Label:", this.changeTextField,
                this.invalidChangesWarningLabel
            };
        
            return arrayMessage;
        }


    }
    
    private void initComponents(){
        Shape s = this.currentActor.getShape();
        
        this.xyLabel = new JLabel("Location: (" + this.currentActor.getTopLeftLocation().x + ", " +
                        this.currentActor.getTopLeftLocation().y + ")");
                
        this.changeTextField = new JTextField(this.currentActor.getActorLabel().getLabelText());
        
        if(s instanceof Rectangle){
            this.widthLbl = new JLabel("Width: " + ((Rectangle) s).width);
            this.heightLbl = new JLabel("Height: " + ((Rectangle) s).height);
        }else if(s instanceof Ellipse2D.Double){
            this.widthLbl = new JLabel("Width: " + ((Ellipse2D.Double) s).width);
            this.heightLbl = new JLabel("Height: " + ((Ellipse2D.Double) s).height);        
        }
    
        this.invalidChangesWarningLabel = new JLabel("");
        this.invalidChangesWarningLabel.setForeground(Color.red);
        
        this.minusHeightBtn = new JButton("-");
        this.minusWidthBtn = new JButton("-");
        this.plusHeightBtn = new JButton("+");
        this.plusWidthBtn = new JButton("+");

        this.minusY = new JButton("- y");
        this.plusY = new JButton("+ y");
        this.minusX = new JButton("- x");
        this.plusX = new JButton("+ x");
        
        
        this.arrayOfLabelPositions = new String[9];
        this.arrayOfLabelPositions[0] = "Left of Top";
        this.arrayOfLabelPositions[1] = "Middle of Top";
        this.arrayOfLabelPositions[2] = "Right of Top";
        this.arrayOfLabelPositions[3] = "Top of Right";
        this.arrayOfLabelPositions[4] = "Middle of Right";
        this.arrayOfLabelPositions[5] = "Bottom of Right";
        this.arrayOfLabelPositions[6] = "Right of Bottom";
        this.arrayOfLabelPositions[7] = "Middle of Bottom";
        this.arrayOfLabelPositions[8] = "Left of Bottom";
        
        
        
        this.minusHeightBtn.addActionListener((ActionEvent e) -> {
            Shape shapeToChange = this.currentActor.getShape();

            if(shapeToChange instanceof Rectangle){
                Point currentShapePoint = ((Rectangle)shapeToChange).getLocation();
                Dimension currentShapeSize = ((Rectangle)shapeToChange).getSize();
                
                Dimension newShapeSize = new Dimension(currentShapeSize.width,
                        currentShapeSize.height - 1);
                
                this.currentActor.setShape(new Rectangle(currentShapePoint, newShapeSize));
            }else if(shapeToChange instanceof Ellipse2D.Double){
                double currentX = ((Ellipse2D.Double)shapeToChange).x;
                double currentY = ((Ellipse2D.Double)shapeToChange).y;
                double currentWidth = ((Ellipse2D.Double)shapeToChange).width;
                double currentHeight = ((Ellipse2D.Double)shapeToChange).height;
                
                Ellipse2D.Double newShape = new Ellipse2D.Double(currentX, currentY,
                        currentWidth, currentHeight - 1.0);
                
                this.currentActor.setShape(newShape);                
            
            }

            this.updateHeightLabel();
            this.updateEditorPane();
            
        });
        
        this.minusWidthBtn.addActionListener((ActionEvent e) -> {
            Shape shapeToChange = this.currentActor.getShape();

            if(shapeToChange instanceof Rectangle){
                Point currentShapePoint = ((Rectangle)shapeToChange).getLocation();
                Dimension currentShapeSize = ((Rectangle)shapeToChange).getSize();
                
                Dimension newShapeSize = new Dimension(currentShapeSize.width - 1,
                        currentShapeSize.height);
                
                this.currentActor.setShape(new Rectangle(currentShapePoint, newShapeSize));
            }else if(shapeToChange instanceof Ellipse2D.Double){
                double currentX = ((Ellipse2D.Double)shapeToChange).x;
                double currentY = ((Ellipse2D.Double)shapeToChange).y;
                double currentWidth = ((Ellipse2D.Double)shapeToChange).width;
                double currentHeight = ((Ellipse2D.Double)shapeToChange).height;
                
                Ellipse2D.Double newShape = new Ellipse2D.Double(currentX, currentY,
                        (currentWidth - 1.0), currentHeight);
                
                this.currentActor.setShape(newShape);                
            
            }

            this.updateWidthLabel();
            this.updateEditorPane();
        });
        
        this.plusHeightBtn.addActionListener((ActionEvent e) -> {
            Shape shapeToChange = this.currentActor.getShape();

            if(shapeToChange instanceof Rectangle){
                Point currentShapePoint = ((Rectangle)shapeToChange).getLocation();
                Dimension currentShapeSize = ((Rectangle)shapeToChange).getSize();
                
                Dimension newShapeSize = new Dimension(currentShapeSize.width,
                        currentShapeSize.height + 1);
                
                this.currentActor.setShape(new Rectangle(currentShapePoint, newShapeSize));
            }else if(shapeToChange instanceof Ellipse2D.Double){
                double currentX = ((Ellipse2D.Double)shapeToChange).x;
                double currentY = ((Ellipse2D.Double)shapeToChange).y;
                double currentWidth = ((Ellipse2D.Double)shapeToChange).width;
                double currentHeight = ((Ellipse2D.Double)shapeToChange).height;
                
                Ellipse2D.Double newShape = new Ellipse2D.Double(currentX, currentY,
                        currentWidth, currentHeight + 1.0);
                
                this.currentActor.setShape(newShape);                
            
            }            
            
            this.updateHeightLabel();
            this.updateEditorPane();
            
        });
        
        this.plusWidthBtn.addActionListener((ActionEvent e) -> {
            Shape shapeToChange = this.currentActor.getShape();

            if(shapeToChange instanceof Rectangle){
                Point currentShapePoint = ((Rectangle)shapeToChange).getLocation();
                Dimension currentShapeSize = ((Rectangle)shapeToChange).getSize();
                
                Dimension newShapeSize = new Dimension(currentShapeSize.width + 1,
                        currentShapeSize.height);
                
                this.currentActor.setShape(new Rectangle(currentShapePoint, newShapeSize));
            }else if(shapeToChange instanceof Ellipse2D.Double){
                double currentX = ((Ellipse2D.Double)shapeToChange).x;
                double currentY = ((Ellipse2D.Double)shapeToChange).y;
                double currentWidth = ((Ellipse2D.Double)shapeToChange).width;
                double currentHeight = ((Ellipse2D.Double)shapeToChange).height;
                
                Ellipse2D.Double newShape = new Ellipse2D.Double(currentX, currentY,
                        (currentWidth + 1.0), currentHeight);
                
                this.currentActor.setShape(newShape);                
            
            }
            
            this.updateWidthLabel();
            this.updateEditorPane();
        });

        this.minusY.addActionListener((ActionEvent e) -> {
            int newY = this.currentActor.getTopLeftLocation().y - 1;
            this.currentActor.setTopLeftLocation(new Point(this.currentActor.getTopLeftLocation().x, newY) );
            this.updateXYLabel();
            this.updateEditorPane();
            
        });
        
        this.minusX.addActionListener((ActionEvent e) -> {
            int newX = this.currentActor.getTopLeftLocation().x - 1;
            this.currentActor.setTopLeftLocation(new Point(newX, this.currentActor.getTopLeftLocation().y) );
            this.updateXYLabel();
            this.updateEditorPane();
            
        });
        
        this.plusY.addActionListener((ActionEvent e) -> {
            int newY = this.currentActor.getTopLeftLocation().y + 1;
            this.currentActor.setTopLeftLocation(new Point(this.currentActor.getTopLeftLocation().x, newY) );
            this.updateXYLabel();
            this.updateEditorPane();
            
        });
        
        this.plusX.addActionListener((ActionEvent e) -> {
            int newX = this.currentActor.getTopLeftLocation().x + 1;
            this.currentActor.setTopLeftLocation(new Point(newX, this.currentActor.getTopLeftLocation().y) );
            this.updateXYLabel();
            this.updateEditorPane();
        });
        
        this.changeActorColorButton = new JButton("Change Actor's Color");
        
        this.changeActorColorButton.addActionListener((ActionEvent e) -> {
            Color c = null, cInit = this.currentActor.getColor();


            c = JColorChooser.showDialog(((Component) e.getSource()).getParent(), "Set Color Of Actor", 
                    cInit);
                    
            if(c != null){
                this.currentActor.setColor(c);
            }
            this.updateEditorPane();
        });        

        
    }

    private void updateHeightLabel(){
        Shape s = this.currentActor.getShape();
        if(s instanceof Rectangle){
            this.heightLbl.setText("Height: " + ((Rectangle)s).height );
        
        }else if(s instanceof Ellipse2D.Double){
            this.heightLbl.setText("Height: " + ((Ellipse2D.Double)s).height );
        
        }
    
    }

    private void updateEditorPane(){   
        
        boolean currentActorIsValid = this.containerUniverse.actorIsValidInContainerUniverse(this.currentActor);
        
        
        if(!currentActorIsValid){
            this.invalidChangesWarningLabel.setText("You cannot apply these changes, they produce an Actor that does not fit in the Container Universe.");
            this.applyChangesToActorButton.setEnabled(false);
        }else{
            this.invalidChangesWarningLabel.setText("");
            this.applyChangesToActorButton.setEnabled(true);
        }
        this.updateIcon();
        
    }

    private void updateWidthLabel(){
        Shape s = this.currentActor.getShape();
        if(s instanceof Rectangle){
            this.widthLbl.setText("Width: " + ((Rectangle)s).width );
        
        }else if(s instanceof Ellipse2D.Double){
            this.widthLbl.setText("Width: " + ((Ellipse2D.Double)s).width );
        
        }
    
    }
    
    private void updateXYLabel(){
        this.xyLabel.setText("Location: (" + this.currentActor.getTopLeftLocation().x + ", " +
                        this.currentActor.getTopLeftLocation().y + ")");
    }
    
    private void updateIcon(){
    
        Color c = this.currentActor.getColor();
        Shape s = this.currentActor.getShape();
        this.setIcon(new ColorIcon(c, s));
    
    }
    
    private class ColorIcon implements Icon{
        
        private final int widthBounds = 80;
        private final int heightBounds = 80;
        private Shape shape;
        private Color color;
        
        public ColorIcon(Color c, Shape s){
            super();
            this.color = c;
            this.shape = s;
        }
        
        @Override
        public void paintIcon(Component c, Graphics g, int x, int y) {
            Graphics2D g2d = (Graphics2D) g.create();

            g2d.setColor(Color.white);
            g2d.drawRect(x, y, widthBounds +2 ,heightBounds +2);
            g2d.setColor(this.color);
            g2d.fill(this.getShapeToDrawWithXY(x, y));

            g2d.dispose();        
        }
        
        private Shape getShapeToDrawWithXY(int x, int y){
            
            
            if(this.shape instanceof Rectangle){
                double heightOverWidthRatio = this.shape.getBounds().getHeight()/
                                                this.shape.getBounds().getWidth();
                int height, width;
                if(heightOverWidthRatio == 1.0){
                    //height == width
                    height = this.heightBounds;
                    width = this.widthBounds;
                }else if(heightOverWidthRatio > 1.0){
                    //height > width
                    height = this.heightBounds;
                    width = (int)((double)height/heightOverWidthRatio);
                }else{
                    //height < width
                    width = this.widthBounds;
                    height = (int)((double)width*heightOverWidthRatio);
                }
                
                Rectangle returnRect = new Rectangle(x, y, width, height);
                return returnRect;
            }if(this.shape instanceof Ellipse2D.Double){
                double heightOverWidthRatio = this.shape.getBounds().getHeight()/
                                                this.shape.getBounds().getWidth();
                
                double width, height;
                
                if(heightOverWidthRatio == 1.0){
                    //height == width
                    width = (double)this.widthBounds;
                    height = (double)this.heightBounds;
                }else if(heightOverWidthRatio > 1.0){
                    //height > width
                    height = (double)this.heightBounds;
                    width = (height/heightOverWidthRatio);
                }else{
                    //height < width
                    width = (double)this.widthBounds;
                    height = width*heightOverWidthRatio;
                }
                Ellipse2D.Double returnEllipse = new Ellipse2D.Double((double)x, (double)y, width, height);
                return returnEllipse;
            }else{     
                //add support for polygons here
                return null;
            }    
            
        }
        
        public void setShape(Shape s){
            this.shape = s;
        }

        @Override
        public int getIconWidth() {
            return this.widthBounds;
        }

        @Override
        public int getIconHeight() {
            return this.heightBounds;
        }
        
    }
    
    
    
}