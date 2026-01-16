/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package tugassemester3;

/**
 *
 * @author RIDHO WAHYU AKMAL
 */
public class Agent {

    private int health;
    private String name;
    private int pos_x, pos_y;

    public Agent() {

    }

    public Agent(String name, int health) {
        this.name = name;
        this.health = health;
    }

    public void SetPos(int x, int y) {
        this.pos_x = x;
        this.pos_y = y;
    }

    public int GetPos() {
        return this.pos_x + this.pos_y;
    }

    public void info() {
        System.out.print("Name: " + this.name);
        System.out.println(" Health: " + this.health);
    }
}