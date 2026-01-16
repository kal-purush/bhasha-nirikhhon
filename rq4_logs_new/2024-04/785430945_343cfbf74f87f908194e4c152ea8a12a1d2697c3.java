package prisonersDilemma;

import mvc.Utilities;
import simstation.*;

public class Prisoner extends Agent {
    private int fitness = 0;
    protected boolean partnerCheated = false;
    private Strategy strategy;
    private Prisoner partner;

    public Prisoner(Strategy s) {
        super();
        strategy = s;
        s.setMyPrisoner(this);
        heading = Heading.random();
        xc = (int) (Math.random() * 415);
        yc = (int) (Math.random() * 500);
    }
    @Override
    public void update() {
        partner = (Prisoner) simulation.getNeighbor(this, 10);
        if (this.strategy.cooperate() && partner.strategy.cooperate()) {
            this.fitness += 3;
            partner.fitness += 3;
        } else if (this.strategy.cooperate() && !partner.strategy.cooperate()) {
            partner.fitness += 5;
        } else if (!this.strategy.cooperate() && partner.strategy.cooperate()) {
            this.fitness += 5;
        } else if (!this.strategy.cooperate() && !partner.strategy.cooperate()) {
            this.fitness += 1;
            partner.fitness += 1;
        }

        heading = Heading.random();
        int steps = Utilities.rng.nextInt(10) + 1;
        move(steps);
    }
}