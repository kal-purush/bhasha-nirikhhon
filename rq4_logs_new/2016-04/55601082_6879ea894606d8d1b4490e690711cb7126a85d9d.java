// Internal action code for project agsgit

package ia;

import jason.asSemantics.DefaultInternalAction;
import jason.asSemantics.TransitionSystem;
import jason.asSemantics.Unifier;
import jason.asSyntax.Term;
import world.World;
import worldObjects.Deposit;
import worldObjects.Glasses;
import worldObjects.Gold;
import worldObjects.Wall;
import worldObjects.Wood;


@SuppressWarnings("serial")
public class setElement extends DefaultInternalAction{
	
	@Override
	public Object execute(TransitionSystem ts, Unifier un, Term[] args) throws Exception{
		try{
			String type = args[0].toString();
			if(type.equals("wood")){
				World	.getInstance()
						.setElement(new Wood(new Integer(args[1].toString()), new Integer(args[2].toString())));
			}else if(type.equals("gold")){
				World	.getInstance()
						.setElement(new Gold(new Integer(args[1].toString()), new Integer(args[2].toString())));
			}else if(type.equals("glasses")){
				World	.getInstance()
						.setElement(new Glasses(new Integer(args[1].toString()), new Integer(args[2].toString())));
			}else if(type.equals("depot")){
				World	.getInstance()
						.setElement(new Deposit(new Integer(args[1].toString()), new Integer(args[2].toString())));
			}else if(type.equals("wall")){
				World	.getInstance()
						.setElement(new Wall(new Integer(args[1].toString()), new Integer(args[2].toString())));
			}else{
				throw new Exception("Wrong item name: " + type);
			}
		}catch(ArrayIndexOutOfBoundsException e){
			System.err.println("Term array out of bounds.");
			return false;
		}catch(Exception e){
			System.err.println(e);
			return false;
		}
		
		// everything ok, so returns true
		return true;
	}
}