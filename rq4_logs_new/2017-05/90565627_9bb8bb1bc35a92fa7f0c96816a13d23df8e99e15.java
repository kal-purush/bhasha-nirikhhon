/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package opengg;

import static org.lwjgl.opengl.GL11.*;

/**
 *
 * @author shun7817
 */
public class Cube3D {
    private float edgeLength;

    public Cube3D(float edgeLength) {
        this.edgeLength = edgeLength;
    }

    public void draw() {
        try {
            glBegin(GL_QUADS);
            glColor3f(0.0f, 0.0f, 1.0f);
            //Top
            glVertex3f(edgeLength, edgeLength, -edgeLength);
            glVertex3f(-edgeLength, edgeLength, -edgeLength);
            glVertex3f(-edgeLength, edgeLength, edgeLength);
            glVertex3f(edgeLength, edgeLength, edgeLength);
            //Bottom         
            glVertex3f(edgeLength, -edgeLength, edgeLength);
            glVertex3f(-edgeLength, -edgeLength, edgeLength);
            glVertex3f(-edgeLength, -edgeLength, -edgeLength);
            glVertex3f(edgeLength, -edgeLength, -edgeLength);
            //Front
            glVertex3f(edgeLength, edgeLength, edgeLength);
            glVertex3f(-edgeLength, edgeLength, edgeLength);
            glVertex3f(-edgeLength, -edgeLength, edgeLength);
            glVertex3f(edgeLength, -edgeLength, edgeLength);
            //Back
            glVertex3f(edgeLength, -edgeLength, -edgeLength);
            glVertex3f(-edgeLength, -edgeLength, -edgeLength);
            glVertex3f(-edgeLength, edgeLength, -edgeLength);
            glVertex3f(edgeLength, edgeLength, -edgeLength);
            //Left
            glVertex3f(-edgeLength, edgeLength, edgeLength);
            glVertex3f(-edgeLength, edgeLength, -edgeLength);
            glVertex3f(-edgeLength, -edgeLength, -edgeLength);
            glVertex3f(-edgeLength, -edgeLength, edgeLength);
            //Right
            glVertex3f(edgeLength, edgeLength, -edgeLength);
            glVertex3f(edgeLength, edgeLength, edgeLength);
            glVertex3f(edgeLength, -edgeLength, edgeLength);
            glVertex3f(edgeLength, -edgeLength, -edgeLength);
            glEnd();
            //Top
            glBegin(GL_LINE_LOOP);
            glVertex3f(edgeLength, edgeLength, -edgeLength);
            glVertex3f(-edgeLength, edgeLength, -edgeLength);
            glVertex3f(-edgeLength, edgeLength, edgeLength);
            glVertex3f(edgeLength, edgeLength, edgeLength);
            glEnd();
            //Bottom         
            glBegin(GL_LINE_LOOP);
            glVertex3f(edgeLength, -edgeLength, edgeLength);
            glVertex3f(-edgeLength, -edgeLength, edgeLength);
            glVertex3f(-edgeLength, -edgeLength, -edgeLength);
            glVertex3f(edgeLength, -edgeLength, -edgeLength);
            glEnd();
            //Front
            glBegin(GL_LINE_LOOP);
            glVertex3f(edgeLength, edgeLength, edgeLength);
            glVertex3f(-edgeLength, edgeLength, edgeLength);
            glVertex3f(-edgeLength, -edgeLength, edgeLength);
            glVertex3f(edgeLength, -edgeLength, edgeLength);
            glEnd();
            //Back
            glBegin(GL_LINE_LOOP);
            glVertex3f(edgeLength, -edgeLength, -edgeLength);
            glVertex3f(-edgeLength, -edgeLength, -edgeLength);
            glVertex3f(-edgeLength, edgeLength, -edgeLength);
            glVertex3f(edgeLength, edgeLength, -edgeLength);
            glEnd();
            //Left
            glBegin(GL_LINE_LOOP);
            glVertex3f(-edgeLength, edgeLength, edgeLength);
            glVertex3f(-edgeLength, edgeLength, -edgeLength);
            glVertex3f(-edgeLength, -edgeLength, -edgeLength);
            glVertex3f(-edgeLength, -edgeLength, edgeLength);
            glEnd();
            //Right
            glBegin(GL_LINE_LOOP);
            glVertex3f(edgeLength, edgeLength, -edgeLength);
            glVertex3f(edgeLength, edgeLength, edgeLength);
            glVertex3f(edgeLength, -edgeLength, edgeLength);
            glVertex3f(edgeLength, -edgeLength, -edgeLength);
            glEnd();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void edge() {

    }

    private void fill() {

    }
}