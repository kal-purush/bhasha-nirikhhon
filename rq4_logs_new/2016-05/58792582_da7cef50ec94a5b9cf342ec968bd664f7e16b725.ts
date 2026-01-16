/**
 * Created by igor on 1/6/16.
 */

type menuItem = { title:string, items?:menuItem[]};
type meneList = menuItem[];

let meneList:meneList = [
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
            {title: 'Скалярии', items: [
                {title: 'Скалярия красная'},
                {title: 'Скалярия черная'},
                {title: 'Скалярия белая'},
            ]}
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


function generateMenu(list:meneList):string {
    let z:string = '';

    for (let group of list) {
        group.items = group.items || [];

        z += `<div class="menu__title"><a class="menu__link">${group.title}</a></div>`;
        z += `<ul class="menu__list">`;

        for (let item of group.items) {
            z += `<li class="menu__item"><a>${item.title}</a>`;
            if (item.items) {
                z += generateMenu(item.items);
            }
            z += `</li>`;
        }

        z += `</ul>`; // .menu__list
    }

    return z;
}


let navMenuList:HTMLElement = document.querySelector('.menu') as HTMLElement;
navMenuList.innerHTML = generateMenu(meneList);
navMenuList.onclick = (e:MouseEvent) => {
    let el = e.target as HTMLElement;
    let classList = el.classList;
    if (classList.contains('menu__link')) {
        let parentLi = el.parentNode as HTMLElement;
        parentLi.classList.toggle('menu__title_open');
    }
}