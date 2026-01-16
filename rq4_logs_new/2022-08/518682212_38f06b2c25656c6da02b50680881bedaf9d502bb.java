package com.distributedSystems.ReplicationJarProject.BackUpCoordinator;

import java.io.Serializable;

public class GlobalRequest implements Serializable {

    public boolean isCommit() {
        return isCommit;
    }

    public void setCommit(boolean commit) {
        isCommit = commit;
    }

    private boolean isCommit;

    public GlobalRequest (boolean isCommit){
        this.isCommit = isCommit;
    }

    @Override
    public String toString() {
        return "{ Commit: "+isCommit+"}";
    }

}