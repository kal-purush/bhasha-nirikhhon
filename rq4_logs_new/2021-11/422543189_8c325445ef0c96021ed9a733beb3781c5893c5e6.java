package hk.edu.polyu.comp.comp2021.clevis.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


class ShapeTest {

    // Change the parameters later..
    Shape squr1 = new Square("squr1",3,3,3);
    Shape squr2 = new Square("squr2",4.132135,5.212312316,8.98765456);
    Shape cir1 = new Circle("cir1", 3, 3, 3);
    Shape cir2 = new Circle("cir1", 5, 5, 5);
    Shape rec1 = new Rectangle("rec1", 3, 3, 5, 7);
    Shape rec2 = new Rectangle("rec2", 6, 6, 8, 9);
    Shape line1 = new Line("line1", 3, 3, 5, 7);
    Shape line2 = new Line("line1", 3, 3, 5, 7);
    Shape[] g = {squr1,squr2};
    Group grp = new Group("x",g);

    @Test
    void addShape() {
        Shape squr1 = new Square("squr1",3,3,3);
        Shape squr2 = new Square("squr2",4.132135,5.212312316,8.98765456);
        Shape cir1 = new Circle("cir1", 3, 3, 3);
        Shape cir2 = new Circle("cir1", 5, 5, 5);
        Shape rec1 = new Rectangle("rec1", 3, 3, 5, 7);
        Shape rec2 = new Rectangle("rec2", 6, 6, 8, 9);
        Shape line1 = new Line("line1", 3, 3, 5, 7);
        Shape line2 = new Line("line1", 3, 3, 5, 7);

        Shape.addShape(squr1);
        Shape.addShape(squr2);
        Shape.addShape(cir1);
        Shape.addShape(cir2);
        Shape.addShape(rec1);
        Shape.addShape(rec2);
        Shape.addShape(line1);
        Shape.addShape(line2);
        //Then the shapes should be in the doubly linked list, idk how you are gonna test that
    }

    @Test
    void getName() {
        assertNotNull(squr1.getName()); //
        assertNotEquals(" ", squr1.getName());
        assertEquals("squr1", (new Square("squr1",3,3,3)).getName());       //do it again pls
    }

    @Test
    void getInfo() {
        squr1.getInfo(1);
        squr2.getInfo(2);
        //it just prints out the information of the Shape in the method of getInfo.

    }

    @Test
    void move() {
        Square x = new Square("x",3,3,3);
        x.move(10,20);
        assertEquals(13,x.x);
        assertEquals(23,x.y);
    }

    @Test
    void boundingbox() {
        double[] res = squr1.boundingbox();
        for(int i = 0; i< 4; i++){
            assertEquals(3,res[i]);
        }
        res = grp.boundingbox();
        assertEquals(3,res[0]);
        assertEquals(5.212312316,res[1]);
        assertEquals(10.11978956,res[2]);
        assertEquals(8.98765456,res[3]);


    }

    @Test
    void ungroup() {
    }

    @Test
    void findAShape() {
    }

    @Test
    void belongToGroup() {
    }

    @Test
    void delete() {
    }

    @Test
    void testDelete() {
    }

    @Test
    void listTest() {
    }

    @Test
    void listFromHead() {
    }
}