import {Selector} from 'testcafe';

import {TodoStatus} from '../src/domains/todos/todosTypes';
import {getThemeLabelByName, Theme} from '../src/components/Styled/themes';

fixture `Todos Page`.page `http://localhost:3000/todos`;

test('when page is loaded displays Todo List',
    async (controller: TestController) => {
        const todoList = Selector('[data-role="todo-list-wrapper"]');
        await controller.expect(todoList.visible).eql(true);
    });

test('switches the status to next one and displays status name in desktop view',
    async (controller: TestController) => {
        const firstStatus = Selector('[data-role="todo-card-status-0"]');
        await controller
            .resizeWindow(1024, 768)
            .expect(firstStatus.innerText).eql(TodoStatus.PENDING)
            .click(firstStatus)
            .expect(firstStatus.innerText).eql(TodoStatus.IN_PROGRESS)
            .click(firstStatus)
            .expect(firstStatus.innerText).eql(TodoStatus.COMPLETE)
            .click(firstStatus)
            .expect(firstStatus.innerText).eql(TodoStatus.PENDING);
    });

test('expands and collapses the cards step list when goal is clicked',
    async (controller: TestController) => {
        const cardGoal = Selector('[data-role="todo-card-goal-0"]');
        const cardStepList = Selector('[data-role="todo-card-step-list-0"]');
        await controller
            .expect(cardStepList.exists).eql(false)
            .click(cardGoal)
            .expect(cardStepList.exists).eql(true)
            .click(cardGoal)
            .expect(cardStepList.exists).eql(false);
    });

test('collapses the first cards step list when another cards goal is clicked',
    async (controller: TestController) => {
        const cardGoal = Selector('[data-role="todo-card-goal-0"]');
        const anotherGoal = Selector('[data-role="todo-card-goal-1"]');
        const cardStepList = Selector('[data-role="todo-card-step-list-0"]');
        const anotherStepList = Selector('[data-role="todo-card-step-list-1"]');
        await controller
            .expect(cardStepList.exists).eql(false)
            .expect(anotherStepList.exists).eql(false)
            .click(cardGoal)
            .expect(cardStepList.exists).eql(true)
            .expect(anotherStepList.exists).eql(false)
            .click(anotherGoal)
            .expect(cardStepList.exists).eql(false)
            .expect(anotherStepList.exists).eql(true);
    });

test('switches the theme',
    async (controller: TestController) => {
        const themeSelect = Selector('[data-role="theme-selector"]');
        const themeSelectOption = themeSelect.find('option');

        await controller
            .expect(themeSelect.value).eql(Theme.LIGHT_THEME)
            .click(themeSelect)
            .click(themeSelectOption.withText(getThemeLabelByName(Theme.DARK_THEME)))
            .expect(themeSelect.value).eql(Theme.DARK_THEME);
    });