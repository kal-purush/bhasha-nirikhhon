export const isUserLoggedIn = () => {
    return window.localStorage.getItem('AUTH-TOKEN') ? true : false;
}