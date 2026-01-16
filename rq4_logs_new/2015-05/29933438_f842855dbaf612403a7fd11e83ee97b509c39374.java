package event;

import sage.ai.behaviortrees.BTAction;
import sage.ai.behaviortrees.BTStatus;

public class ChaseAvatar extends BTAction {
    private NPC npc;
    
    public ChaseAvatar(NPC npc) {
        this.npc = npc;
    }
    
    @Override
    protected BTStatus update(float elapsedTimeMS) {
        npc.updateLocation(elapsedTimeMS);
        return BTStatus.BH_SUCCESS;
    }
}