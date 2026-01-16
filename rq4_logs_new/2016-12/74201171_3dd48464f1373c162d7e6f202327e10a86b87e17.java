package Pkg;

/**
 * Created by gimy on 12/12/2016.
 */
public class ANDOR_Node implements SqlStatementNode{
    private String ANDOR;

    public ANDOR_Node(String ANDOR) {
        this.ANDOR = ANDOR;
    }

    public String getANDOR() {
        return ANDOR;
    }

    @Override
    public int getType() {
        return SqlStatementNode.ANDOR_Node;
    }
}