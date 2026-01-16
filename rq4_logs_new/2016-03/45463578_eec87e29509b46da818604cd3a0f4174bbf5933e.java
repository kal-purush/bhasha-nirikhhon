package Models.Parameters.ConditionalStimulus.Rodriguez;

import Constants.ParameterNamingConstants;
import Models.Parameters.ConditionalStimulus.CsParameter;

import java.io.Serializable;

/**
 * Created by Rokas on 29/03/2016.
 */
public class InitialAssociationParameter extends CsParameter implements Serializable {
    public InitialAssociationParameter(String cueName) {
        super(cueName, ParameterNamingConstants.INITIAL_ASSOCIATION);
    }
}