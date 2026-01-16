package DesignPattern.AbstractFactoryPattern;

import DesignPattern.AbstractFactoryPattern.Factorys.AbstractFactory;
import DesignPattern.AbstractFactoryPattern.Factorys.FactoryProducer;
import DesignPattern.AbstractFactoryPattern.colors.Color;
import DesignPattern.AbstractFactoryPattern.shapes.Circle;
import DesignPattern.AbstractFactoryPattern.shapes.Rectangle;
import DesignPattern.AbstractFactoryPattern.shapes.Shape;
import DesignPattern.AbstractFactoryPattern.shapes.Square;

public class AbstractFactoryPatternDemo {

    public static void main(String[] args) {
        //获取形状工厂
        AbstractFactory shapeFactory = FactoryProducer.getFactory("SHAPE");
        //获取形状为 Circle 的对象
        Shape circle = (Circle) shapeFactory.getShape(Circle.class);
        //调用 Circle 的 draw 方法
        circle.draw();
        //获取形状为 Rectangle 的对象
        Shape rect = (Rectangle) shapeFactory.getShape(Rectangle.class);
        //调用 Rectangle 的 draw 方法
        rect.draw();
        //获取形状为 Square 的对象
        Shape square = (Square) shapeFactory.getShape(Square.class);
        //调用 Square 的 draw 方法
        square.draw();
        //获取颜色工厂
        AbstractFactory colorFactory = FactoryProducer.getFactory("COLOR");
        //获取颜色为 Red 的对象
        Color color1 = colorFactory.getColor("RED");
        //调用 Red 的 fill 方法
        color1.fill();
        //获取颜色为 Green 的对象
        Color color2 = colorFactory.getColor("Green");
        //调用 Green 的 fill 方法
        color2.fill();



        //
        Shape shape1 = shapeFactory.getShape2(Circle.class);
        shape1.draw();
        Shape shape2 = shapeFactory.getShape2(Rectangle.class);
        shape2.draw();
        Shape shape3 = shapeFactory.getShape2(Square.class);
        shape3.draw();
    }
}