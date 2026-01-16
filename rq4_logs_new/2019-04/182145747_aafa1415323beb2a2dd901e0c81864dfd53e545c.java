package norms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by XXX on 8/14/18.
 */
public class Actor extends NormativeAgent {

    private static final Logger log = LogManager.getLogger(Actor.class);

    public HashMap<List<String>,Double> normComplianceRate; //Key: [context,norm], value: adherence* prevalance(0/1 whether agent will comply) (This is what the agent uses to act)

    public Actor(HashMap<String,IndexedDSFrame> normBoEs, HashMap<List<String>,Double> normComplianceRate, String startingContext){
        super();
        this.mentalBoEs = normBoEs;
        // Set Agent Type
        this.agentType = AgentType.ACTOR;
        // Set Actor compliance rate
        this.normComplianceRate = normComplianceRate; // this will be computed when citizens are being created.
        // Set starting context
        this.startingContext = startingContext;
    }

    @Override
    public void sense(){
        // Determine and set current context
        String c = determineCurrentContext();
        setCurrentContext(c);
    }

    /**
     * Simple agent action model.
     * Based on adherence*prevalence for action.
     */
    @Override
    public void act(){
        log.trace("Actor acting");

        // Get the norms that apply in this context
        IndexedDSFrame relevantBoE = mentalBoEs.get(currentContext);
        List<String> relevantNorms = relevantBoE.getAttributes();

        currentActionsPerformed.clear();
        // For each normative action decide on compliance
        for (String norm : relevantNorms){
            //Get compliance rate for norm
            List<String> key = new ArrayList<>();
            key.add(currentContext);
            key.add(norm);
            Double rate  = normComplianceRate.get(key);
            // Flip biased coin
            if(Utility.flipBiasedCoin(rate)){
                //perform action if true
                //update current actions performed
                currentActionsPerformed.put(norm,true);  // T
            } else {
                currentActionsPerformed.put(norm,false);
            }
        }
    }


    /**
     * Simple case here of setting the current context to the starting context
     * @return
     */
    private String determineCurrentContext(){
        String c = startingContext;
        return c;
    }
}