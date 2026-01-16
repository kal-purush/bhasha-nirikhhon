'use strict';

function displayResults(responseJSon) {
    console.log(response.Json);
    //remove previous results
    $('#results-list').empty();
    //iterates through results, lists name of repos with link
    for(let i = 0; i < responseJson.length; i++) {
        $('#results-list').append(
            `<li><h3>${responseJson[i].name}</h3>
            <p><a href = "${response.Json[i].html_url}</a></p>
            <p>${responseJson[i].description}</p>
            </li>`
        )   
    }

    $('#results').removeClass('hidden');
}


function getUser(username) {
    console.log('getUser ran')
    const url = `https://api.github.com/users/${username}/repos`
    fetch(url)
        .then(response => {
            if (response.ok) {
                return response.json();
            }
            throw new Error(response.statusText);
        })
        .then(responseJson => displayResults(responseJson))
        .catch(err => {
            $('#js-error-message').text(`Something Went wrong: ${err.message}`);
        });
}
    
function watchForm() {
    $('#js-form').submit(event => {
        event.preventDefault();
        const searchName = $('#js-search-name').val();
        getUser(searchName);
    })
}

$(getUser);