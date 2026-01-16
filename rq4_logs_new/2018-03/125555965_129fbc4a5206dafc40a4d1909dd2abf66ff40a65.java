package assignment4;

import assignment4.Critter.TestCritter;

/* The Morty Critter's life has always been down.
   It only knows being down, and so it moves downward.
   It spits out babies downwards. It always runs away
   from fights; however, when Morty is on the same spot as
   a Rick Critter, Morty becomes enraged and fights him.
 */

public class Critter1 extends TestCritter {

    @Override
    public String toString () { return "M"; }

    @Override
    public void doTimeStep() {
        walk(6);
        run(6);
        if (getEnergy() >= Params.min_reproduce_energy) {
            Critter1 child = new Critter1();
            reproduce(child, 6);
        }
    }

    @Override
    public boolean fight(String opponent) {
        if (opponent.equals("R")) {
            return true;
        }
        return false;
    }

}