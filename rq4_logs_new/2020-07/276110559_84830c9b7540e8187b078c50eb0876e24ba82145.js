const getData = () => new Promise((resolve, reject) => {
    const data = JSON.parse(window.localStorage.getItem('list') || '[]');
    resolve(data);
});
const updateData = ({ id, value }) => new Promise((resolve, reject) => {
    const data = JSON.parse(window.localStorage.getItem('list') || '[]');
    const currentData = data.find(v => v.id === id);
    currentData.value = value;
    window.localStorage.setItem('list', JSON.stringify(data));
    resolve(data);
});
const saveData = (value) => new Promise((resolve, reject) => {
    var _a;
    const data = JSON.parse(window.localStorage.getItem('list') || '[]');
    const newData = { id: (((_a = data.slice(-1)[0]) === null || _a === void 0 ? void 0 : _a.id) || 0) + 1, value };
    const news = [...data, newData];
    window.localStorage.setItem('list', JSON.stringify(news));
    resolve(news);
});
const deleteData = (id) => new Promise((resolve, reject) => {
    const data = JSON.parse(window.localStorage.getItem('list') || '[]');
    const newData = data.filter(v => v.id !== id);
    window.localStorage.setItem('list', JSON.stringify(newData));
    resolve(newData);
});
const template = `
  <div data-id="{{id}}">
    <input disabled type="text" value="{{value}}">
    <button class="delete">删除</button>
  </div>
`;
const render = (data) => {
    $('.content').innerHTML = data.map(v => template.replace('{{value}}', v.value).replace('{{id}}', v.id.toString())).join('');
};
const $ = selector => document.querySelector(selector);
const $$ = selector => [...document.querySelectorAll(selector)];
getData().then((data) => render(data));
$('#app').addEventListener('click', (e) => {
    const targetElement = e.target;
    if ($$('.add').includes(targetElement)) {
        const inputElement = $('.input');
        const value = inputElement.value;
        if (value === '')
            return;
        saveData(value).then((data) => {
            inputElement.value = '';
            render(data);
        });
    }
    if ($$('.delete').includes(targetElement)) {
        deleteData(+targetElement.parentElement.getAttribute('data-id')).then((data) => render(data));
    }
});