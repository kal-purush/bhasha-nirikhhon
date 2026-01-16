console.log("Cristian es Ghe")

const cookieStorage = {
    getItem: (key) =>{

        const cookies = document.cookie
                    .split(";")
                    .map(cookie => cookie.split("="))
                    .reduce((acc, [key,value]) => ({...acc, [key.trim()]: value}))
                    return cookies[key];
    },
    setItem: (key, value) => {
        document.cookie = `${key} = ${value}`
    }
}

const storageType = cookieStorage;
const consetPropertyName = "jdc_consent";



const shouldShowPopup = () => !storageType.getItem(consetPropertyName);
const saveToStorage = () => storageType.setItem(consetPropertyName, true);


window.onload = () => {
    if(shouldShowPopup()){
        const consent = confirm("Please Agree to this consent in order to use the website");
        if(consent){
            saveToStorage();
        }
    }
    
}