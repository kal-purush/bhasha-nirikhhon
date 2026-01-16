public class Identifier implements IdentifierInterface{

    StringBuffer buffer;

    Identifier(){
        buffer = new StringBuffer();
    }

    public void add (char toAdd){
        buffer.append(toAdd);
    }

    public boolean equals (StringBuffer identifier){
        return buffer.toString().equals(identifier.toString());
    }

    public StringBuffer getIdentifier(){
        return buffer;
    }

    public int getSize(){
        return buffer.length();
    }

    public void print(){
        System.out.print(buffer + "\n");
    }
}