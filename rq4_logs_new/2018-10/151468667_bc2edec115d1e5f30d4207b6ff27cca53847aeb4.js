export const handleChange = e => ({
    type: 'HANDLE_CHANGE',
    payload: e.target.value
})

const getGithubUserEvents = (username) => {
    return fetch(`https://api.github.com/users/${username}/events`)
};

export const handleLogin = events => ({
    type: 'LOGIN',
    payload: events
})

export const login = username => dispatch => {
    getGithubUserEvents(username)
    .then(res => res.json())
    .then(data => dispatch(handleLogin(data)))
    console.log(`Button clicked!`)
};