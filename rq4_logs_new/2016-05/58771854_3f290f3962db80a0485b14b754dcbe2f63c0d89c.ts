type menuListItem = { title:string, items?:menuListItem[] };

let muneList:menuListItem[] = [
    {
        title: `Животные`,
        items: [
            {title: `Кошки`},
            {title: `Cобаки`}
        ]
    },
    {
        title: `Рыбы`,
        items: [
            {
                title: `Рыбы 2`,
                items: [
                    {
                        title: `Рыбы 3`,
                        items: [
                            {
                                title: `Рыбы 4`,
                                items: [
                                    {title: `Акулы 5`},
                                    {title: `Лососи 5`}
                                ]
                            },
                            {title: `Лососи`}
                        ]
                    },
                    {title: `Лососи 2`}
                ]
            },
            {title: `Лососи`}
        ]
    }
];

function generateItem(item:menuListItem):string {
    const subItems:menuListItem[] = Array.isArray(item.items) ? item.items : [];
    const linkClassName:string = subItems.length ? 'title' : 'single-item-title';
    const subMenu:string = generateMenu(subItems);
    return `<li><a class="${linkClassName}">${item.title}</a>${subMenu}</li>`;
}

function generateMenu(list:menuListItem[]):string {
    return list.length ? `<ul>${list.map(generateItem).join('')}</ul>` : '';
}


let navMenuList:HTMLElement = document.querySelector('.menu') as HTMLElement;
navMenuList.innerHTML = generateMenu(muneList);
navMenuList.onclick = (e:MouseEvent) => {
    let el = e.target as HTMLElement;
    let classList = el.classList;
    if (classList.contains(`title`)) {
        let parentLi = el.parentNode as HTMLElement;
        parentLi.classList.toggle('menu-open');
    }
};