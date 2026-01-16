const API = 'https://test-users-api.herokuapp.com/';
const usersContainer = document.querySelector('.our_users');
const buttonCreate = document.querySelector('#create');
const loadUsers = document.querySelector('.load');

const getUsers = () => {
    return fetch(API + 'users', {
        method: 'GET'
    }).then(res => {
        return res.json();
    }).catch(err => {
        console.log('no user found', err);
        return [];
    });
};

const deleteUser = async (userId, userItem) => {
    try {
        const deleteUser = await fetch(API + 'users/' + userId, {
            method: 'DELETE'
        });
        userItem.remove();
    } catch (err) {
        console.log(err);
    }
};

const renderUsers = (users) => {

    users.data.forEach(item => {
        const userItem = document.createElement('div');
        userItem.classList.add('user-card');
        userItem.innerHTML =
            `<p>Name: ${item.name}</p>
		<span>Age: ${item.age}</span>
		`;
        const removeUser = document.createElement('img');
        removeUser.classList.add('user-delete');
        removeUser.src = 'img/delete.svg';
        removeUser.addEventListener('click', () => {
            deleteUser(item.id, userItem);
            console.log('deleted name:', item.name)
            console.log('deleted id:', item.id)
        })
        userItem.append(removeUser);
        usersContainer.append(userItem);
    });
}

const init = async () => {
    const users = await getUsers();
    renderUsers(users);
    console.log('Our users:', users);
}

const createUser = () => {
    const name = document.querySelector('#name').value;
    const age = document.querySelector('#age').value;

    fetch(API + 'users', {
        method: 'POST',
        body: JSON.stringify({
            name: name,
            age: age
        })
    }).then(() => {
        renderUsers({ data: [{ name: name, age: age }] });
    }).catch(err => {
        console.log(err);
    })
};

loadUsers.addEventListener('click', init);
buttonCreate.addEventListener('click', createUser);