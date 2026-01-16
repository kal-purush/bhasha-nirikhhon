export const checkVlidateData = (name, email, password) => {
    const isName = name.length > 3;
    const isEmailValid = /^[a-zA-Z0-9_.±]+@[a-zA-Z0-9-]+.[a-zA-Z0-9-.]+$/.test(email);
    const isPassValid = /^(?=.*?[A-Z])(?=.*?[a-z])(?=.*?[0-9])(?=.*?[#?!@$%^&*-]).{8,}$/.test(password);

    if (!isName) return "Name must be grater than 3 character";
    if (!isEmailValid) return "Email is not valid";
    if (!isPassValid) return "Password is not valid";

    return null;
}