package it.unitn.lingProg.ronchet.esercitazione3a;

import java.util.Objects;

enum Valore {
    A,
    _2,
    _3,
    _4,
    _5,
    _6,
    _7,
    _8,
    _9,
    _10,
    J,
    Q,
    K
};

enum Seme {
    Cuori, Quadri, Fiori, Picche
}

public class Carta implements Comparable {

    Valore valore;
    Seme seme;

    public Carta(Seme seme, Valore valore) {
        this.valore = valore;
        this.seme = seme;
    }

    @Override
    public int hashCode() {
        //return Objects.hash(valore,seme);
        int hash = 3;
        hash = 41 * hash + Objects.hashCode(this.valore);
        hash = 41 * hash + Objects.hashCode(this.seme);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final Carta other = (Carta) obj;
        if (this.valore != other.valore)
            return false;
        if (this.seme != other.seme)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "[" + valore + "," + seme + ']';
    }

    @Override
    public int compareTo(Object obj) {
        if (this == obj)
            return 0;
        if (obj == null || getClass() != obj.getClass())
            System.err.println("Error in compareTo " + obj);
        final Carta other = (Carta) obj;
        int v_comp = this.valore.compareTo(other.valore);
        //int v_comp=other.seme.ordinal()-this.seme.ordinal();
        if (v_comp != 0)
            return v_comp;
        int s_comp = this.seme.compareTo(other.seme);
        return s_comp;
    }

    public static void main(String a[]) {
        Seme c=Seme.Cuori;
        Seme p=Seme.Picche;
        System.out.println(c.name()+" "+p.name());
        System.out.println(c+" "+p);
        System.out.println(c.ordinal()+" "+p.ordinal());
        System.out.println(c.compareTo(p));
        for (Seme x:Seme.values()) {
            System.out.println(x);
        }
        System.exit(1);
        Carta c1 = new Carta(Seme.Picche, Valore.K);
        Carta c2 = new Carta(Seme.Cuori, Valore.K);
        System.out.println(c2);
        System.out.println(c1.compareTo(c2));
    }
}