/* tslint:disable */
var menu:{ title:string, link?:string, items?:typeof menu}[];
/* tslint:enable */

type menuOpt = {element:HTMLElement,menuList:typeof menu};

class Menu {

    public toggle(id:number):void {
        let el = document.getElementById('menu-item-'+id);
        el.classList.toggle('menu-open');
        while (el.parentElement != this.getElem() ) {
            el = el.parentElement;
            el.classList.toggle('menu-open');
        }
    }

    public close(id:number):void {
        let el = document.getElementById('menu-item-'+id);
        el.classList.toggle('menu-open');
        while (el.parentElement != this.getElem() ) {
            el = el.parentElement;
            el.classList.remove('menu-open');
        }
    }

    public open(id:number):void {
        let el = document.getElementById('menu-item-'+id);
        el.classList.toggle('menu-open');
        while (el.parentElement != this.getElem() ) {
            el = el.parentElement;
            el.classList.add('menu-open');
        }
    }

    private clickHandler(e:MouseEvent):void {
        let el = <HTMLElement>e.target;
        let classList = el.classList;
        if (classList.contains('title')) {
            let parentLi = <HTMLElement>el.parentNode;
            parentLi.classList.toggle('menu-open');
        }
    }

    private element:HTMLElement;
    private menuList:typeof menu;
    
    private idCounter:number = 0;

    constructor(opt:menuOpt) {
        this.element = opt.element;
        this.menuList = opt.menuList;
        this.element.innerHTML = this.generateMenuHtml(this.menuList);
        this.element.addEventListener('click', this.clickHandler);
    }
    
    public getElem ():HTMLElement {
        return this.element;
    }

    private generateMenuHtml(menuList:typeof menu):string {
        let z:string = `<ul>`;
        for (let a of menuList) {
            z += `<li id="menu-item-${this.idCounter++}"><a ${a.items ? 'class=title' : ''} ${a.link ? 'href=' + a.link : ''}>${a.title}</a>`;
            if (!a.items) {
                z += `</li>`;
                continue;
            }
            z += this.generateMenuHtml(a.items) + `</li>`;
        }
        return z += `</ul>`;
    }
}


menu = [
    {
        title: 'Животные', items: [
        {
            title: 'Млекопитающие', items: [
            {title: 'Коровы'},
            {title: 'Ослы'},
            {title: 'Собаки'},
            {title: 'Тигры'}
        ]
        },
        {
            title: 'Другие', items: [
            {title: 'Змеи'},
            {title: 'Птицы'},
            {title: 'Ящерицы'},
        ],
        },
    ]
    },
    {
        title: 'Рыбы', items: [
        {
            title: 'Аквариумные', items: [
            {title: 'Гуппи'},
            {title: 'Скалярии'}
        ]
        },
        {
            title: 'Форель', items: [
            {title: 'Морская форель'}
        ]
        },
    ]
    }
];

let element = <HTMLElement>document.querySelector('.menu');


let menu1 = new Menu({element: element, menuList: menu});

document.getElementById('toggle').onclick = () => {menu1.toggle(2)};
document.getElementById('open').onclick = () => {menu1.open(12)};
document.getElementById('close').onclick = () => {menu1.close(12)};