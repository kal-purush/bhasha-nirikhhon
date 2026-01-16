//package plague;
//
//import simstation.Agent;
//import simstation.Simulation;
//import java.util.Arrays;
//
//public class PlagueSimulation extends Simulation {
//
//    public static int VIRULENCE = 50;
//    public static int RESISTANCE = 2;
//
//    public PlagueSimulation() {
//        super();
//        populate();
//    }
//
//    @Override
//    public void populate() {
//        for (int i = 0; i < 49; i++) {
//            Agent newAgent = new PlagueAgent("PlagueAgent", this);
//            agents.add(newAgent);
//        }
//
//        //make an infected agent
//        Agent newAgent = new PlagueAgent("PlagueAgent", this, true);
//        agents.add(newAgent);
//
//        changed();
//
//    }
//
//    @Override
//    public String[] getStats() {
//        String[] old = super.getStats();
//        String[] newStats = Arrays.copyOf(old, old.length + 3);
//        int infectedAgents = 0;
//        for (Agent a : agents) {
//            PlagueAgent agent = (PlagueAgent) a;
//            if (agent.isInfected())
//                infectedAgents++;
//        }
//
//        newStats[old.length] = "Infected Agents: " + infectedAgents;
//        newStats[old.length + 1] = "Healthy Agents: " + (agents.size() - infectedAgents);
//        newStats[old.length + 2] = "%infected: " + (float) infectedAgents / agents.size() * 100;
//        return newStats;
//    }
//}