import structure5.*;

public class Interpreter{
    
    //instance variables
    protected StackVector<Token> stack;
    protected SymbolTable table; 
    protected Double first;
    protected Double sec;

    public Interpreter(){
	stack = new StackVector<Token>();
	table = new SymbolTable();
    }

    //a clean way of printing out stack
    public void print(){
	String s = "";
	for (Token d : stack){
	    s += d + ", ";
	}
	//gets rid of the extra ", "
	if (s.length()>0) s=s.substring(0,s.length()-2);
	System.out.println(s);
    }

    public void get2Nums(){
	Assert.pre(stack.size()>1,
		   "Stack too small.");
	Token a = stack.pop();
	Token b = stack.pop();
	Assert.pre(a.kind()==Token.NumberKind &&
		   b.kind()==Token.NumberKind,
		   "Need 2 numbers.");
	sec = a.getNumber();
	first = b.getNumber();
    }
    
    public void interpret(Reader r){
	Token t;
	while(r.hasNext()){
	    t = r.next();
	    //the interpretations:
	    switch (t.kind()) {
	    case Token.ProcedureKind:
		
		break;
	    case Token.BooleanKind:
		stack.add(t);
		break;
	    case Token.NumberKind:
		stack.add(t);
		break;
	    case Token.SymbolKind:
		String input = t.getSymbol();
		switch (input){
		case "quit":
		    System.exit(0);
		    break;
		case "pstack":
		    print();
		    break;
		case "pop":
		    stack.pop();
		    break;
	        case "add":
		    get2Nums();
		    stack.push(new Token(first+sec));
		    break;
		case "sub":
		    get2Nums();
		    stack.push(new Token(first-sec));
		    break;
		case "mul":
		    get2Nums();
		    stack.push(new Token(first*sec));
		    break;
		case "div":
		    get2Nums();
		    stack.push(new Token(first/sec));
		    break;
		case "dup":
		    Token a = stack.peek();
		    stack.push(a);
		    break;
		case "exch":
		    Assert.pre(stack.size()>1,
			       "Stack too small.");
		    a = stack.pop();
		    Token b = stack.pop();
		    stack.push(a);
		    stack.push(b);
		    break;
		case "eq":
		    Assert.pre(stack.size()>1,
			       "Stack too small.");
		    a = stack.pop();
		    b = stack.pop();
		    stack.push(new Token(a.equals(b)));
		    break;
		case "ne":
		    Assert.pre(stack.size()>1,
			       "Stack too small.");
		    a = stack.pop();
		    b = stack.pop();
		    stack.push(new Token(!a.equals(b)));
		    break;
		case "ptable":
		    System.out.println(table);
		    break;
		case "def":
		    Assert.pre(stack.size()>1,
			       "Stack too small.");
		    a = stack.pop(); //the value
		    b = stack.pop(); //the symbol
		    Assert.pre(b.kind()==Token.SymbolKind,
			       "Invalid def operation.");
		    String symbol = b.getSymbol().substring(1);
		    table.add(symbol,a);
		    break;
		default:
		    String s = t.getSymbol();
		    if (s.substring(0,1).equals("/")) stack.push(t);
		    else{
			if (table.contains(s))
			    stack.push(table.get(s));
		    }
		    break;
		}
		break;
	    }
	}
    }

    public static void main(String[] args){
	Interpreter i = new Interpreter();
	while (true){
	    i.interpret(new Reader());
	}
    }
}

