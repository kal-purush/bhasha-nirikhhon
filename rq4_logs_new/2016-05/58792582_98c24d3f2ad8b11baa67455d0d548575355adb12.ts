/**
 * Created by igor on 1/2/16.
 */
/* tslint:disable */
var menu: { title: string, link?: string, items?: typeof menu}[];
/* tslint:enable */
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

function toDOMTree(menuList: typeof menu): string {
    let z: string = `<ul>`;
    for (let a of menuList) {
        z += `<li><a ${a.items ? 'class=title' : ''} ${a.link ? 'href=' + a.link : ''}>${a.title}</a>`;
        if (!a.items) {
            z += `</li>`;
            continue;
        }
        z += toDOMTree(a.items) + `</li>`;
    }
    return z += `</ul>`;
}
let navMenu: HTMLElement = <HTMLElement>document.querySelector('.menu');
navMenu.innerHTML = toDOMTree(menu);
navMenu.onclick = (e: Event) => {
    let el = <HTMLElement>e.target;
    let classList = el.classList;
    if (classList.contains('title')) {
        let parentLi = <HTMLElement>el.parentNode;
        parentLi.classList.toggle('menu-open');
    }
};

