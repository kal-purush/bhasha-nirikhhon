/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.terisproj;

/**
 *
 * @author Winter Fox
 */
public class Game {
    private ShapeTable playground;
    private int score;
    private boolean endOfGame;
    
    private Shape currentShape;
    private int koordinate_x, koordinate_y;
    
    public Game(int w, int h){
        playground = new ShapeTable(w, h);
        score=0;
        endOfGame=false;
        currentShape=Shape.getRandom();
        koordinate_x=(w-currentShape.getWidth())/2;
        koordinate_y=0;
        
        drawEmptyGround();
        
    }
    
    //Ширина игрового поля
    public int getWidth(){
        return playground.getWidth();
    }
    //Высота игрового поля
    public int getHeight(){
        return playground.getHeight();
    }
    
    private void drawEmptyGround(){
        playground.fill(0, 0, playground.getWidth(), playground.getHeight(), 0);
    }
    
    public ShapeTable getStateNow(){
        ShapeTable state = new ShapeTable(playground);
        state.copy(currentShape, koordinate_x, koordinate_y);
        return state;
    }
    
    //Каждый раз проверяет можно ли падающей фигуре двигаться вниз
        //Если да, то он двигается на 1 клетку вниз
            //Остальное вроде бы понятно
    public boolean oneMove(){
        if(!playground.fitsInto(currentShape, koordinate_x, koordinate_y+1)){
            playground.copy(currentShape, koordinate_x, koordinate_y);
        
            for (int j=playground.getHeight()-1;j>=0;j--)
                while(lineIsFull(j)==true){
                    flushLine(j);
                    score+=10;
                }
            currentShape = Shape.getRandom();
            koordinate_x=(playground.getWidth()-currentShape.getWidth())/2;
            koordinate_y=0;
            if(!playground.fitsInto(currentShape, koordinate_x, koordinate_y))
                this.endOfGame = true;
        }
        else{
            koordinate_y++;
        }
        return endOfGame;
    }
    
    //Очистка линии (ПРОВЕРКА)
    private boolean lineIsFull(int y){
        for(int x=0; x<playground.getWidth();x++){
            if (playground.isEmpty(x, y)){
                return false;
            }
        }
        return true;
    }
    
    //Очистка линии
    private void flushLine(int y){
        for(int j=y;j>0;j--){
            for(int i=0;i<playground.getWidth();i++){
                playground.set(i, j, playground.get(i,j-1));
            }
        }
        
        for(int i=0; i<playground.getWidth();i++){
            playground.set(i, 0, 0);
        }
    }
    
    public int getScore(){
        return score;
    }
    
    public boolean isEndOfGame(){
        return endOfGame;
    }
    
    //Сдвиг влево, если возможно
    public void goLeft(){
        if(!playground.fitsInto(currentShape, koordinate_x-1, koordinate_y)){
            return;
        }
        koordinate_x--;
    }
    
    //Сдвиг вправо, если возможно
    public void goRight(){
        if(!playground.fitsInto(currentShape, koordinate_x+1, koordinate_y)){
            return;
        }
        koordinate_x++;
    }
    
    //Вращать
    public void rotate(){
        Shape shape = new Shape(currentShape);
        shape.rotate();
        if(playground.fitsInto(shape, koordinate_x, koordinate_y)){
            currentShape = shape;
        }
    }
    
    //Бросить вниз
    public void fall(){
        while(playground.fitsInto(currentShape, koordinate_x, koordinate_y)){
            koordinate_y++;
        }
    }
    
    
}