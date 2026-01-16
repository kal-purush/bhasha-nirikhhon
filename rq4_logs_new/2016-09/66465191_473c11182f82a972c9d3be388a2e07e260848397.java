/**
 * Created by Michael on 06/09/2016.
 */
public class PlayerNode<Player> {
    private PlayerNode Parent;
    private PlayerNode Left;
    private PlayerNode Right;
    private PlayerNode Middle;
    private Player value;//would be player

    public PlayerNode(Player Value){
        this.value = Value;
        this.Left = null;
        this.Middle = null;
        this.Right = null;
    }

    public PlayerNode(PlayerNode L, PlayerNode M, PlayerNode R, Player Value){
        this.value = Value;
        this.Left = L;
        this.Middle = M;
        this.Right = R;
    }

    public PlayerNode(PlayerNode P, PlayerNode L, PlayerNode M, PlayerNode R, Player Value){
        this.value = Value;
        this.Left = L;
        this.Middle = M;
        this.Right = R;
        this.Parent = P;
    }

    public void setChildren(PlayerNode L, PlayerNode M, PlayerNode R)
    {
        this.Left = L;
        this.Middle = M;
        this.Right = R;
    }

    public void setParent(PlayerNode P) {
        this.Parent = P;
    }

    public PlayerNode getLeft() {
        if(this.Left!= null)
            return Left;
        return null;//return to root
    }

    public void bestChild()
    {
        if(!isLeaf())
        {
            Player Left = (Player)this.getLeft().value;
            Player Middle = (Player)this.getMiddle().value;
            Player Right = (Player)this.getRight().value;
        }
    }

    public void setLeft(PlayerNode left) {
        this.Left = left;
    }

    public PlayerNode getRight() {
        if(this.Right!= null)
            return Right;
        return null;//return to root
    }

    public void setRight(PlayerNode right) {
        this.Right= right;
    }

    public PlayerNode getMiddle() {
        if(this.Middle!= null)
            return Middle;
        return null;//return to root
    }

    public void setMiddle(PlayerNode middle) {
        Middle = middle;
    }

    public boolean isLeaf() {
        if(this.getMiddle() == null)
            return true;
        else
            return false;
    }
}