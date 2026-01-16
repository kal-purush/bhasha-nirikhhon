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
package Examples;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java2dscrollinguniverse.Controller.UniverseController;
import java2dscrollinguniverse.Model.TwoDimensionalMovement;
import java2dscrollinguniverse.Model.actors.Actor;
import java2dscrollinguniverse.Model.actors.ActorLabel;
import java2dscrollinguniverse.Model.actors.ActorLabel.PositionOfLabel;
import java2dscrollinguniverse.Model.actors.ActorType;
import java2dscrollinguniverse.SettingsSingleton;
import java2dscrollinguniverse.SettingsSingleton.ViewScrollMode;
import javax.swing.JLabel;
import javax.swing.SwingUtilities;

/**
 *
 * @author andrewnyhus
 */
public class Football {
    public static Dimension game_size = new Dimension(340, 600);
    public static JLabel ballLocationLabel;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here

        Color transparentColor = new Color(255, 255, 255, 0);
        Color backgroundGrassColor = new Color(26, 108, 0);
        Color solidWhiteColor = new Color(255, 255, 255);

        Actor[] yardLines = generateYardLines(game_size);
        
        Actor[] listOfInitialMembers = new Actor[2+yardLines.length];

        for(int x = 0; x < yardLines.length; x++){
            listOfInitialMembers[x] = yardLines[x];
        }
        
        listOfInitialMembers[yardLines.length] = generateMidField(Color.WHITE);

        listOfInitialMembers[yardLines.length + 1] = generateFootball();
        
        SwingUtilities.invokeLater(() -> {
            
            SettingsSingleton.getInstance().setContainerBackgroundColor(backgroundGrassColor);
            SettingsSingleton.getInstance().setPerimeterColor(solidWhiteColor);
            
            SettingsSingleton.getInstance().setShouldShowHUDMap(false);            
            SettingsSingleton.getInstance().setCenterOfViewActorColor(transparentColor);            
            
            SettingsSingleton.getInstance().setScrollMode(ViewScrollMode.DONT_SCROLL);
            
            SettingsSingleton.getInstance().setWindowDimension(game_size);

            UniverseController mainController = new UniverseController("Football", game_size, listOfInitialMembers);
            
            
        });
    }

    public static Actor generateMidField(Color c){
    
        int diameter = game_size.width/4;

        Actor midField = new Actor(ActorType.miscObject,new Point(game_size.width/2 - diameter/2,
                                                                    game_size.height/2 - diameter/2),
                                                c, new Ellipse2D.Double(0, 0, diameter, diameter));
        
        return midField;
    }

    public static Actor generateFootball(){

        TwoDimensionalMovement initialVelocity = new TwoDimensionalMovement(1, 1);

        Color brown = new Color(140, 40, 40);
        Point location = new Point(game_size.width/2, game_size.height/2);
        
        Ellipse2D.Double ballShape = new Ellipse2D.Double(0, 0, 60, 25);

        Actor mainLace = new Actor(ActorType.miscObject, new Point(15, 11),
                                                        Color.WHITE, new Rectangle(0, 0, 30, 3));

        Actor[] childActors = new Actor[9];
        childActors[0] = mainLace;
        
        for(int i = 0; i < 8; i++){
            int width = 1;
            int height = 5;
            
            int xLoc = 15 + i*(30/7);
            int yLoc = 11 - (height/3);
            
            Actor verticalLace = new Actor(ActorType.miscObject, new Point(xLoc,yLoc),
                                                    Color.WHITE, new Rectangle(0, 0, width, height));
            childActors[i+1] = verticalLace;

        }

        
        Actor football = new Actor(ActorType.miscObject, location, brown, ballShape, childActors);
        football.setVelocity(initialVelocity);
        return football;
    }

    public static Actor[] generateYardLines(Dimension fieldDimensions){

        int width = fieldDimensions.width - 20;
        int height = fieldDimensions.height - 20;//accounting for perimeter 

        int equivTenYards = 10*(height/110);
        
        int NUM_LINES = 11;
        String[] labelStringsForLine = new String[NUM_LINES];
        Actor[] arrayOfLines = new Actor[NUM_LINES];

        labelStringsForLine[0] = "Touchdown!";
        labelStringsForLine[1] = "10";
        labelStringsForLine[2] = "20";
        labelStringsForLine[3] = "30";
        labelStringsForLine[4] = "40";
        labelStringsForLine[5] = "50";
        labelStringsForLine[6] = "40";
        labelStringsForLine[7] = "30";
        labelStringsForLine[8] = "20";
        labelStringsForLine[9] = "10";
        labelStringsForLine[10] = "Touchdown!";
        
        
        
        for(int i = 0; i < NUM_LINES; i++){
            int x = 10;
            int y = equivTenYards*(i+1);
            PositionOfLabel position = PositionOfLabel.LEFT_OF_BOTTOM;

            if(i==0)
                position = PositionOfLabel.RIGHT_OF_TOP;
            
            Actor currLine = new Actor(ActorType.miscObject, new Point(x, y),
                                       Color.WHITE, new Rectangle(0, 0, width, 3),null,
                                        new ActorLabel(labelStringsForLine[i], position));
            
            SettingsSingleton.getInstance().setLabelColor(Color.WHITE);
            arrayOfLines[i] = currLine;
        }
        
        return arrayOfLines;
    }
    
}