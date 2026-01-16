let urlSearch = new URLSearchParams(window.location.search);
let userId = urlSearch.get('id');

fetch(`https://jsonplaceholder.typicode.com/posts/${userId}`)
    .then(response => response.json())
    .then(post => {
        const postBlok = document.createElement('div');
        postBlok.classList.add('postBlok');

        const userIdDiv = document.createElement('div');
        userIdDiv.innerText = `User ID: ${post.userId}`;
        userIdDiv.classList.add('postAll');
        postBlok.appendChild(userIdDiv);

        const titleDiv = document.createElement('div');
        titleDiv.innerText = `Title: ${post.title}`;
        titleDiv.classList.add('postAll');
        postBlok.appendChild(titleDiv);

        const bodyDiv = document.createElement('div');
        bodyDiv.innerText = `Body: ${post.body}`;
        bodyDiv.classList.add('postAll');
        postBlok.appendChild(bodyDiv);

        const idDiv = document.createElement('div');
        idDiv.innerText = `ID: ${post.id}`;
        idDiv.classList.add('postAll');
        postBlok.appendChild(idDiv);

        document.body.appendChild(postBlok);
    })
    .catch(error => console.error(error));


let urlSearchComent = new URLSearchParams(window.location.search);
let userIdComent = urlSearchComent.get('id');
fetch(`https://jsonplaceholder.typicode.com/posts/${userIdComent}/comments`)
    .then(response => response.json())
    .then(comments => {
        const boxComent = document.createElement('div');
        boxComent.classList.add('boxComent');

        for (let i = 0; i < comments.length; i++) {
            const commentDiv = document.createElement('div');
            commentDiv.classList.add('comment');

            const commentName = document.createElement('h3');
            commentName.innerText = comments[i].name;
            commentDiv.appendChild(commentName);

            const commentEmail = document.createElement('p');
            commentEmail.innerText = comments[i].email;
            commentDiv.appendChild(commentEmail);

            const commentBody = document.createElement('p');
            commentBody.innerText = comments[i].body;
            commentDiv.appendChild(commentBody);
            boxComent.appendChild(commentDiv);

        }
        document.body.appendChild(boxComent);
    });