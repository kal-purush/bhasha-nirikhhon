import { Task, PerformsTasks, Click } from 'serenity-js/protractor';
import { TodoList } from './../components/todo_list';

export class MarkATodoItemAsDone implements Task {

    constructor(private itemName: string) {}

    static called(itemName): MarkATodoItemAsDone {
        return new MarkATodoItemAsDone(itemName);
    };

    performAs(actor: PerformsTasks): PromiseLike<void> {
        return actor.attemptsTo(
            Click.on(TodoList.Checkbox_Of_Item_With_Name(this.itemName))
        );
    };
}