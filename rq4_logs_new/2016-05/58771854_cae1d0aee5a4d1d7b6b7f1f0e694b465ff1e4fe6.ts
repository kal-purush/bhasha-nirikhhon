
type muneList = { title:string,items?:muneList}[];

let muneList:muneList = [
    {
        title: 'Животные', 
        items: [
                    {
                        title: 'Млекопитающие', 
                        items: [
                            {title: 'Коровы',
                                items: [
                                {title:'Пятнистая'},
                                {title:'Белая'},
                                {title:'Черная'}
                            ]},
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
                            {title: 'Ящерицы',
                                items: [
                                    {title:'Красная'},
                                    {title:'Синяя'},
                                    {title:'Зеленая'}
                                ]}
                        ]
                    }
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
            {title: 'Морская форель', 
                items: [
                    {title: 'Маленькая'},
                    {title: 'Средняя'},
                    {title: 'Большая'}
                ]},
            {title: 'Речная форель', items: [
                {title: 'Маленькая'},
                {title: 'Средняя'},
                {title: 'Большая', items: [{title:'И еще уровень'}]}
            ]}
        ]
        },
    ]
    }
];

function generateMenu(list:muneList):string {
    let z:string = `<ul>`;
    for (let a of list){
        if (Array.isArray(a.items))
        {
            z += `<li>
                    <a class="title">${a.title}</a>`;
            z += generateMenu(a.items);
        }
        else
        {
            z += `<li>
                    ${a.title}`;
        }
        z += `</li>`;
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