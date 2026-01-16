class FlaggedCell extends Cell {

    public FlaggedCell(int y, int x)
    {
        super(y, x);
    }

    public Type getType(){
        return Type.FlaggedCell;
    }
}