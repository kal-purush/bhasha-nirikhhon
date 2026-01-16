package prisonersDilemma;

import mvc.Model;
import simstation.*;

import java.awt.*;

public class PrisonersDilemmaView extends SimulationView {
    public PrisonersDilemmaView(Model model) {
        super(model);
        setBackground(Color.WHITE);
    }

    @Override
    public void paintComponent(Graphics gc) {
        super.paintComponent(gc);
        setBackground(Color.WHITE);
        PrisonersDilemmaSimulation prisonerSimulation = (PrisonersDilemmaSimulation) model;
        for (Agent a : prisonerSimulation.getAgents()) {
            Prisoner p = (Prisoner) a;
            String strategy = p.strategy.getStrategy();
            switch (strategy) {
                case "Cooperate":
                    gc.setColor(Color.GREEN);
                    gc.fillOval(p.getXc(), p.getYc(), 10, 10);
                    break;
                case "RandomlyCooperate":
                    gc.setColor(Color.CYAN);
                    gc.fillOval(p.getXc(), p.getYc(), 10, 10);
                    break;
                case "Cheat":
                    gc.setColor(Color.RED);
                    gc.fillOval(p.getXc(), p.getYc(), 10, 10);
                    break;
                case "Tit4Tat":
                    gc.setColor(Color.ORANGE);
                    gc.fillOval(p.getXc(), p.getYc(), 10, 10);
                    break;
                default:
                    break;
            }
        }
    }
}