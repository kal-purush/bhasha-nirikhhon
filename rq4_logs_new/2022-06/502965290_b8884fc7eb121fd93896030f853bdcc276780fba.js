const base = 'http://localhost:8000/api'
let token = ''

const roadmap = () => {
    fetch(base + '/roadmap', {
        method: 'POST',
        headers: new Headers({
            Accept: 'application/json',
            Authorization: "Bearer " + token
        }),
        body: JSON.stringify({
            book_id: 1
        })
    })
        .then((data => data.json()))
        .then((data) => console.log(data.road_map))
}

fetch(base + '/register', {
    method: 'POST',
    headers: new Headers({
        Accept: 'application/json'
    }),
    body: JSON.stringify({
        name: 'user',
        email: 'user@example.ru',
        password: 'test',
        password_confirmation: 'test',
        role_id: 1
    })
})
    .then(result => result.json())
    .then((data) => {
        token = data.token
    })

if (token == null) {
    console.log('smth wrong')
}

fetch(base + '/logout', {
    method: 'POST',
    headers: new Headers({
        Accept: 'application/json',
        Authorization: "Bearer " + token
    })
})
    .then((data) => data.json())
    .then((msg) => console.log(msg.message))


fetch(base + '/login', {
    method: 'POST',
    headers: new Headers({
        Accept: 'application/json'
    }),
    body: JSON.stringify({
        email: 'user@example.ru',
        password: 'test'
    })
})
    .then((response) => response.json())
    .then((data) => {
        token = data.token
        console.log(data)
    })

fetch(base + '/book/create', {
    method: 'POST',
    headers: new Headers({
        Accept: 'application/json',
        Authorization: "Bearer " + token
    }),
    body: JSON.stringify({
        name: 'book1',
        content: 'some conntent',
        redactor_id: 1,
        illustrator_id: 3,
    })
})
    .then((resp) => resp.json())
    .then(msg => console.log(msg))

fetch(base + '/book/all', {
    headers: new Headers({
        Accept: 'application/json',
        Authorization: "Bearer " + token
    })
})
    .then(resp => resp.json())
    .then(data => console.log(data.books))

fetch(base + '/book/name', {
    method: 'POST',
    headers: new Headers({
        Accept: 'application/json',
        Authorization: "Bearer " + token
    }),
    body: {
        book: 1,
        name: "book2"
    }
})
    .then(msg => msg.json())
    .then(data => console.log(data))
roadmap()
fetch(base + '/book/content', {
    method: 'POST',
    headers: new Headers({
        Accept: 'application/json',
        Authorization: "Bearer " + token
    }),
    body: {
        book: 1,
        name: "concatenated content"
    }
})
    .then(msg => msg.json())
    .then(data => console.log(data))
roadmap()
fetch(base + '/book/illustrator', {
    method: 'POST',
    headers: new Headers({
        Accept: 'application/json',
        Authorization: "Bearer " + token
    }),
    body: {
        book: 1,
        illustrator: 4
    }
})
    .then(msg => msg.json())
    .then(data => console.log(data))
roadmap()
fetch(base + '/book/redactor', {
    method: 'POST',
    headers: new Headers({
        Accept: 'application/json',
        Authorization: "Bearer " + token
    }),
    body: {
        book: 1,
        redactor: 5
    }
})
    .then(msg => msg.json())
    .then(data => console.log(data))

roadmap()

fetch(base + '/book/author?id=1', {
    headers: new Headers({
        Accept: 'application/json',
        Authorization: "Bearer " + token
    }),
})
    .then(response => response.json())
    .then(data => console.log(data))

fetch(base + '/book/get?id=1', {
        headers: new Headers({
                Accept: 'application/json',
                Authorization: "Bearer " + token
            }
        )
    }
)
    .then(response => response.json())
    .then(data => console.log(data))

fetch(base + '/users/all', {
        headers: new Headers({
                Accept: 'application/json',
                Authorization: "Bearer " + token
            }
        )
    }
)
    .then(response=>response.json())
    .then(response=>console.log(response))

fetch(base+'/users/aworks?id=1', {
    headers: new Headers({
            Accept: 'application/json',
            Authorization: "Bearer " + token
        }
    )
})
    .then(data=>data.json())
    .then(data=>console.log(data))

fetch(base+'/users/rworks?id=1', {
    headers: new Headers({
            Accept: 'application/json',
            Authorization: "Bearer " + token
        }
    )
})
    .then(data=>data.json())
    .then(data=>console.log(data))

fetch(base+'/users/lworks?id=1', {
    headers: new Headers({
            Accept: 'application/json',
            Authorization: "Bearer " + token
        }
    )
})
    .then(data=>data.json())
    .then(data=>console.log(data))


roadmap()

