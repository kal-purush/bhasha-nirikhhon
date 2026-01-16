package core.models;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@EqualsAndHashCode(of = "name")
public class Cook {
    private CookType type;
    private CookState state;

    public void cook(Pizza pizza) throws InterruptedException {
        synchronized (this){
            this.state = CookState.busy;
       synchronized (pizza){
           var cookTime = pizza.getPreparationTime()/3*1000;

           if(pizza.getState() ==PizzaState.created && type==CookType.makingDough){
               pizza.setState(PizzaState.dough);
               wait(cookTime);
               pizza.setState(PizzaState.filling);
           } else if (pizza.getState() ==PizzaState.filling && type==CookType.filling) {
               System.out.println(pizza.getState()+" "+cookTime);
               wait(cookTime);
               pizza.setState(PizzaState.baking);
           }else if (pizza.getState() ==PizzaState.baking && type==CookType.backing) {
               wait(cookTime);
               pizza.setState(PizzaState.ready);
           }else if(type==CookType.multi){
               pizza.setState(PizzaState.dough);
               System.out.println("dough");
               wait(cookTime);
               pizza.setState(PizzaState.filling);
               System.out.println("filling");
               wait(cookTime);
               pizza.setState(PizzaState.baking);
               System.out.println("baking");
               wait(cookTime);
               System.out.println("ready");
               pizza.setState(PizzaState.ready);
           }
           notifyAll();
           this.state = CookState.free;

       }
        }

    }
}