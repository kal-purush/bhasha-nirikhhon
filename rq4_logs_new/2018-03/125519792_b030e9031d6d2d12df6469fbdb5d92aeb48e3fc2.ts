import { Enter, PerformsTasks, Task} from 'serenity-js/protractor';
import { protractor } from 'protractor';
import { TodoList } from './../components/todo_list';

export class AddATodoItem implements Task {
    
    constructor(private itemName: string) { };

    static called(itemName: string) {
        return new AddATodoItem(itemName);
    };

    performAs(actor: PerformsTasks): PromiseLike<void> {
        return actor.attemptsTo(
            Enter.theValue(this.itemName)
                .into(TodoList.What_Needs_To_Be_Done)
                .thenHit(protractor.Key.ENTER)
        );
    };
}