/**
 * Created by igor on 1/6/16.
 */

type menuList = { title:string, id?:string, items?:menuList }[];

interface MenuInterface {
    getElem():HTMLElement;
    toggle(id:string):void;
    close(id:string):void;
    open(id:string):void;
    render(parent?:HTMLElement):void;
}

class Menu implements MenuInterface {
    private element: HTMLElement;
    private menuList: menuList;
    private id: number;
    static increment: number = 0;

    constructor(menuList: menuList) {
        this.menuList = menuList;
        this.id = Menu.increment++;
    }

    public getElem():HTMLElement {
        return this.element;
    }

    public toggle(id:string):void {
        let elem:HTMLElement = this.getItemById(id);
        elem.classList.toggle('menu-open');
    }

    public close(id:string):void {
        let elem:HTMLElement = this.getItemById(id);
        elem.classList.remove('menu-open');
    }

    public open(id:string):void {
        let elem:HTMLElement = this.getItemById(id);
        elem.classList.add('menu-open');
    }

    public render(parent?:HTMLElement):void {
        let parentElement:HTMLElement = parent || document.body;

        this.element = document.createElement('div');
        this.element.classList.add('menu');
        this.element.innerHTML = this.generateMenu(this.menuList);

        parentElement.appendChild(this.element);
        this.element.addEventListener('click', Menu.clickHandler);
    }

    static clickHandler(e:MouseEvent):void {
        let el = <HTMLElement>e.target;
        let classList = el.classList;
        if (classList.contains('title')) {
            let parentLi = <HTMLElement>el.parentNode;
            parentLi.classList.toggle('menu-open');
        }
    }

    private getItemById(id:string):HTMLElement {
        return document.getElementById(id +'-'+ this.id);
    }

    private generateMenu(items:menuList):string {
        let html:string = '';
        if (items && !Array.isArray(items)) {
            throw new TypeError('variable "items" must be like an Array');
        }

        if (!items || items.length === 0) {
            return html;
        }

        html += `<ul>`;
        for (let item of items) {

            let id:string = item.id,
                title:string = item.title,
                items:menuList = item.items,
                hasItems:boolean = !!(items && items.length);

            html += `<li${id ? ` id="${id}-${this.id}"` : ''}>
                <a class="title${hasItems ? '' : '-leaf'}">${title}</a>
                ${this.generateMenu(items)}
            </li>`;

        }
        html += `</ul>`;
        return html;
    }
}

let menuList:menuList = [
    {
        title: 'Животные',
        id: 'animals',
        items: [
            {
                title: 'Млекопитающие',
                items: [
                    {title: 'Коровы'},
                    {title: 'Ослы'},
                    {title: 'Собаки'},
                    {title: 'Тигры'}
                ]
            },
            {
                title: 'Другие',
                items: [
                    {title: 'Змеи'},
                    {title: 'Птицы'},
                    {title: 'Ящерицы'}
                ]
            }
        ]
    },
    {
        title: 'Рыбы',
        id: 'fish',
        items: [
            {
                title: 'Аквариумные',
                items: [
                    {title: 'Гуппи'},
                    {title: 'Скалярии'}
                ]
            },
            {
                title: 'Форель',
                items: [
                    {title: 'Морская форель'}
                ]
            }
        ]
    }
];

let menu = new Menu(menuList);
let menu1 = new Menu(menuList);

menu.render();
menu1.render();