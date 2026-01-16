package lab1;

import java.util.Objects;

public class Edge {

    private char a, b;
    private int weight;


    public Edge() {
    }

    public Edge(char a, char b, int weight) {
        this.a = a;
        this.b = b;
        this.weight = weight;
    }

    public void setA(char a) {
        this.a = a;
    }

    public void setB(char b) {
        this.b = b;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public char getA() {
        return a;
    }

    public char getB() {
        return b;
    }

    public int getWeight() {
        return weight;
    }

    public boolean havePoint(char a){
        if (this.a == a){
            return true;
        }
        if (this.b == a){
            return true;
        }
        return false;
    }
    @Override
    public String toString() {
        return "����� {" + a + " " + b + " " + weight + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Edge edge = (Edge) o;
        return a == edge.a &&
                b == edge.b &&
                weight == edge.weight;
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b, weight);
    }
}