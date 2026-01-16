/**
 * Created by cookx876 on 2/14/17.
 */

import { ComponentFixture, TestBed, async } from "@angular/core/testing";
import {Todo} from "./todo";
import {TodoComponent} from "./todo.component";
import {TodoService} from "./todo.service";
import { PipeModule } from "../../pipe.module";
import { Observable } from "rxjs";
// import {FilterBy} from "./filter.pipe";

describe('testing things', () => {
    let todoList: TodoComponent;
    let fixture: ComponentFixture<TodoComponent>;
    let todoServiceStub: {
        getTodos: () => Observable<Todo[]>
    };


    beforeEach(() => {
        todoServiceStub = {
            getTodos: () => Observable.of([
                {
                    id: "588959855aac378a2f7119ff",
                    owner: "Roberta",
                    status: true,
                    body: "Incididunt elit cillum laborum sunt sit veniam ullamco sit laboris veniam nulla. Labore labore occaecat dolore et fugiat in do nisi eu incididunt dolor officia adipisicing.",
                    category: "software design"
                },
                {
                    id: "58895985e131bd26d0576031",
                    owner: "Fry",
                    status: false,
                    body: "Ea id cupidatat magna sint aliquip ut voluptate. Esse occaecat amet id aliquip commodo.",
                    category: "video games"
                },
                {
                    id: "588959851dcf63c371007691",
                    owner: "Dawn",
                    status: true,
                    body: "Do cillum ipsum esse duis. Labore do ea nisi nisi ut occaecat sint consequat.",
                    category: "video games"
                },
                {
                    id: "58895985079310e4deeb1444",
                    owner: "Fry",
                    status: true,
                    body: "Adipisicing quis eu dolore mollit labore id nostrud. Mollit eu officia consectetur ea labore commodo ea.",
                    category: "groceries"
                },
                {
                    id: "58895985ee196f2401e8c52a",
                    owner: "Roberta",
                    status: false,
                    body: "In sunt adipisicing tempor non aliquip ad reprehenderit aute do aliquip deserunt nostrud aute aliquip. Ipsum irure anim excepteur proident irure cillum Lorem occaecat in non non.",
                    category: "software design"
                },
                {
                    id: "58895985313a7b3a51ca2b40",
                    owner: "Fry",
                    status: false,
                    body: "Anim anim anim non ea consequat amet occaecat nisi est sunt. Eiusmod aliquip nulla duis elit nostrud aute nostrud ex ut proident non.",
                    category: "groceries"
                },
                {
                    id: "58895985bc042a142189b3ff",
                    owner: "Barry",
                    status: true,
                    body: "Ea fugiat eu exercitation laboris incididunt nulla ullamco qui ad nisi quis pariatur. Exercitation aliqua eiusmod ut velit mollit incididunt do aliquip ad.",
                    category: "groceries"
                }
            ])
        };

    TestBed.configureTestingModule({
        imports: [PipeModule],
        declarations: [TodoComponent ],
        providers: [{provide: TodoService, useValue: todoServiceStub}]
    })
    });

    beforeEach(async(() => {
        TestBed.compileComponents().then(() => {
            fixture = TestBed.createComponent(TodoComponent);
            todoList = fixture.componentInstance;
            fixture.detectChanges();
        });
    }));



    it('should add things correctly', () =>{
        expect(2+3).toBe(5);
    })
    it('should return the correct number of todos', () =>{
        expect(todoList.todos.length).toBe(7);
    })
    it("Contains an owner named 'Fry'" , () =>{
        expect(todoList.todos.some((todo:Todo) => todo.owner === "Fry" )).toBe(true);
    })
    it("Does not contain an owner named 'Santa'" , () =>{
        expect(todoList.todos.some((todo:Todo) => todo.owner === "Santa" )).toBe(false);
    })
    it("has three todos that have the category groceries", () =>{
        expect(todoList.todos.filter((todo:Todo) => todo.category === "groceries" ).length).toBe(3);
    })
    it("has zero todos that have the category math", () =>{
        expect(todoList.todos.filter((todo:Todo) => todo.category === "math" ).length).toBe(0);
    })
    it("has four complete todos", () =>{
        expect(todoList.todos.filter((todo:Todo) => todo.status === true ).length).toBe(4);
    })
    it("has three incomplete todos", () =>{
        expect(todoList.todos.filter((todo:Todo) => todo.status === false ).length).toBe(3);
    })
    it("contains a certain Id", () =>{
        expect(todoList.todos.some((todo:Todo) => todo.id === "58895985bc042a142189b3ff" )).toBe(true);
    })
    it("Does not contain a certain Id", () =>{
        expect(todoList.todos.some((todo:Todo) => todo.id === "88895985bc042a142189b3ff" )).toBe(false);
    })
    it("contains a certain string in the body", () =>{
        expect(todoList.todos.filter((todo:Todo) => todo.body === "In sunt adipisicing tempor non aliquip ad reprehenderit aute do aliquip deserunt nostrud aute aliquip. Ipsum irure anim excepteur proident irure cillum Lorem occaecat in non non." ).length).toBe(1);
    })
    it("Does not contain a certain string in the body", () =>{
        expect(todoList.todos.filter((todo:Todo) => todo.body === "bananas" ).length).toBe(0);
    })

})


