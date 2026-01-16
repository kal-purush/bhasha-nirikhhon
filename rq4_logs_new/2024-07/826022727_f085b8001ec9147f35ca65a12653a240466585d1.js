let collectionButton = document.getElementsByClassName('collection-button');

const id = document.querySelector('.card-id');
const image = document.querySelector('.card-image');
const name = document.querySelector('.card-title');
const types = document.querySelector('card-title');

for (let i=0; i < collectionButton.length; i++) {
    collectionButton[i].addEventListener('click', function (event) {
    event.preventDefault();

    const response = fetch(`/`, {
        method: 'POST',
        body: JSON.stringify({ id, name, image, types}),
        headers: {
            'Content-Type': 'application/json',
        }, 
    });

    if (response.ok) {
        alert('Congrats! Added to collection!')
      } else {
        alert(response.statusText);
      }

})
}