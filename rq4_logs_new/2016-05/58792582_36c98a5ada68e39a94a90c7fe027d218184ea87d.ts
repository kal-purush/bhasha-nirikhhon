/**
 * показалось интересным оставить возможность некоторые пункты указывать строками
 * правда пришлось прочитать про Type Guards
 */
type titledItem = {title: String, items?: menuItem[]};
type menuItem = titledItem | String;
type menuList = menuItem[];

let menuList:menuList = [
    {title: `Животные`,
        items: [`Кошки`,
                `Cобаки`,
                {title: `Коровы`},
                {title: `Дикие`,
                    items: [`Волки`,`Медведи`]}]},
    {title: `Рыбы`,
        items: [
            `Акулы`,
            `Лососи`
        ]}
];

function isTitledItem(item: menuItem): item is titledItem {
    return (<titledItem>item).title !== undefined;
}

function generateMenu(list: menuList) : string {
    if(!list || !list.length){
        return '';
    }
    let result:string = '<ul>';
    for(let item of list){
        if(isTitledItem(item)){
            let hasItems = Array.isArray(item.items) && item.items.length;
            result += `<li><a` + (hasItems ? ` class="title" ` : '') + `>${item.title}</a>`;
            if(hasItems){
                result += generateMenu(item.items);
            }
            result += `</li>`;
        }
        else{ // is simple String
            result += `<li><a>${item}</a>`;
        }
    }
    result += '</ul>';
    return result;
}


let navMenuList:HTMLElement = document.querySelector('.menu') as HTMLElement;
navMenuList.innerHTML = generateMenu(menuList);
navMenuList.onclick = (e:MouseEvent) => {
    let el = e.target as HTMLElement;
    let classList = el.classList;
    if (classList.contains(`title`)) {
        let parentLi = el.parentNode as HTMLElement;
        parentLi.classList.toggle('menu-open');
    }
};