const myButton = document.querySelector('#myButton')
const nameInput = document.querySelector('#nameInput')
const list = document.querySelector('.list')
const searchInput = document.querySelector('#searchInput')

myButton.addEventListener('click', () => {
    const newItem = document.createElement('li');

    list.appendChild(newItem);

    newItem.classList.add('item')

    newItem.textContent = nameInput.value;
    nameInput.value = ' '

    const delBtn = document.createElement('Button');
    delBtn.textContent = 'x';

    newItem.appendChild(delBtn);

    delBtn.addEventListener('click', () => {
        list.removeChild(newItem);
    });

    newItem.addEventListener('click', () => {

    })
})

const items = document.querySelectorAll('item')

function search(input) {
    let value = input.value;
    for(let item of items) {
        if(isMatching(item.textContent, value) == false){
            item.style.display = 'none';
        } else {
            item.style.display = 'flex';
        }
    }
}

myBtn.addEventListener('click', () => {
    search(searchInput);
}) 