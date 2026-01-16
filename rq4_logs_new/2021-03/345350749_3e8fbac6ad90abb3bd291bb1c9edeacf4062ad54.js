window.onload = () => {
    function $(el) { return document.querySelector(el) };

    const listContainer = $('.list-container');
    const btnAdd = $('.btnAdd');
    const btnSave = $('.btnSave');
    const listWrap = $('.listWrap');

    function render() {
        //сделать запрос в локальное хранилище
        let data = localStorage.getItem('data');

        //проверить, что данные существуют и доступны
        if (data != null && data != undefined) {

            //парсить дату и отрисовать ее
            let objectData = JSON.parse(data);
            let innerLi = "";
            for (i = 0; i < objectData.length; i++) {
                let title = objectData[i].title;
                let value = objectData[i].value;
                let content = `<li><span class="dataEdit dataTitle">${title}</span>: <span class="dataEdit dataValue">${value}</span></li>`;
                innerLi += content;
            }
            listContainer.innerHTML = innerLi;

            const dataEdit = document.querySelectorAll('.dataEdit');
            dataEdit.forEach(item => {
                item.addEventListener('dblclick', (e) => {
                    let currentItem = e.target;
                    currentItem.innerHTML = `<input type="text" placeholder="${currentItem.innerText}">`
                    currentItem.childNodes[0].focus();


                    const deliteListener = () => {
                        document.removeEventListener('click', listener, true);
                        document.removeEventListener('keydown', listener, true);
                    }

                    const listener = (e) => {
                        if (e.target != currentItem.childNodes[0] || e.code === "Enter") {
                            let newValue = currentItem.childNodes[0].value;
                            currentItem.innerHTML = newValue;
                            deliteListener();
                        }
                    }

                    document.addEventListener('click', listener, true);
                    document.addEventListener('keydown', listener, true)
                })
            })

        } else { listContainer.innerHTML = '<li>Нет данных в хранилище</li>' }
    }

    render();

    //добавление задачи в проект
    btnAdd.addEventListener('click', () => {
        let el = document.createElement('div');
        el.innerHTML = '<li><input class="dataTitle" type="text" placeholder="Задача"> <input class="dataValue" type="text" placeholder="Значение"></li>';
        listWrap.appendChild(el);
    })

    //сохранение изменений
    const saveData = () => {
        localStorage.removeItem('data');
        let newData = [];
        const newTitle = [...document.querySelectorAll('.dataTitle')];
        const newValue = [...document.querySelectorAll('.dataValue')];

        for (i = 0, j = 0; i < newTitle.length, j < newValue.length; i++, j++) {
            newData.push({ title: newTitle[i].innerText || newTitle[i].value, value: newValue[j].innerText || newValue[j].value })
        }
        localStorage.setItem('data', JSON.stringify(newData));
        listWrap.innerHTML = '';
        render();

    }

    //сохранение новых данных
    btnSave.addEventListener('click', () => { saveData() })
}