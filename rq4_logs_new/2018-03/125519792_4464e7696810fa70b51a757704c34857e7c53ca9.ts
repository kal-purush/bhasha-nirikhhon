import { Click, Task, PerformsTasks, step, Wait, Duration } from 'serenity-js/protractor';
import { TodoList } from './../components/todo_list';
import { Hover } from './../../../utils/helpers';

export class RemoveATodoItem implements Task {
    
    constructor(private itemName: string) { }

    static called(itemName: string): RemoveATodoItem {
        return new RemoveATodoItem(itemName);
    };

    @step('{0} removes a Todo item called #itemName')
    performAs(actor: PerformsTasks): PromiseLike<void> {
        return actor.attemptsTo(
            Hover.over(TodoList.Item_With_Name(this.itemName)),
            Click.on(TodoList.Remove_Button)
        );
    };
}