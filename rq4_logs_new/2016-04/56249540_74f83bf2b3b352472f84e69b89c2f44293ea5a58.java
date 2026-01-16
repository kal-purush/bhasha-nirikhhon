/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CanvasToDraw.WithShape.shape;

import java.awt.Graphics;
import java.awt.geom.Point2D;
import java.util.List;

/**
 *
 * @author ΙΩΑΝΝΑ
 */
public interface IItems {
    List<Point2D> getPoints();
    void doDrawing(Graphics g);  
    int getSize(); 
}