/**
 * Created by igor on 1/6/16.
 */

type muneList = { title:string,items:string[]}[];

let muneList:muneList = [
    {title: `Животные`, items: [`Кошки`, `Cобаки`]},
    {title: `Рыбы`, items: [`Акулы`, `Лососи`]}
];

function generateMenu(list:muneList):string {
    let z:string = `<ul>`;
    for (let a of list) {
        z += `<li><a class="title">${a.title}</a>`;
        z += `<ul>`;
        for (let item of a.items) {
            z += `<li><a>${item}</a></li>`;
        }
        z += `</ul></li>`;
    }
    z += `</ul>`;
    return z;
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
}