import { Container, Service } from "typedi"
import { DomainEvents, EventHandler } from "../../infrastructure/events"
import { UserCreatedEvent } from "../users/events"
import { CreateTodoByUserService } from "./create-todo-by-user/create-todo.service";
import { ICreateTodo } from "./todo";

@Service(`${UserCreatedEvent.eventName}Handler`)
class WelcomeTodoEventHandler extends EventHandler<UserCreatedEvent> {
  constructor(private readonly createTodoByUserService: CreateTodoByUserService) {
    super()
  }

  async handle(event: UserCreatedEvent) {
    const todoProps = {
      userId: event.id,
      description: `Welcome ${event.name}`
    } as ICreateTodo

    const result = await this.createTodoByUserService.execute(todoProps);

    result.unwrap(
      (todo) => {
        console.log(`Welcome todo with id ${todo.id} create successfully!`);
      },
      (error) => { throw error; }
    );
  }
}


@Service()
export class TodoEventHandlers {
  constructor(private readonly domainEvents: DomainEvents) {
    console.log("this.domainEvents", domainEvents)
  }

  init() {
    this.domainEvents.on(UserCreatedEvent.eventName, (e) => {
      Container.get<EventHandler<UserCreatedEvent>>(`${UserCreatedEvent.eventName}Handler`).handle(e)
    });
  }
}