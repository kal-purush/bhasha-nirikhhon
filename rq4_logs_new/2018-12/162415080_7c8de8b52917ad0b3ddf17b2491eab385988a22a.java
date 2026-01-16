package wow.alliance;

import wow.AbstractWOWCharacter;

public abstract class AbstractAllianceCharacter extends AbstractWOWCharacter {
    AbstractAllianceCharacter(String className) {
        super(className);
        fractionName = "AbstractAllianceCharacter";

    }
}