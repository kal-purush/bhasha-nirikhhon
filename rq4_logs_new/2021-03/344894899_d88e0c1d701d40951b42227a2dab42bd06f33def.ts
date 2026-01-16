export default async function sendUserInfo (data:Object, url:string) {
    try {
        const response = await fetch(url, {
            method: "POST",
            body: JSON.stringify(data), 
            headers: {
                'Content-Type': 'application/json'
            }
        });
        const json = await response.json();
        console.log("Submit");
    } catch (error) {
        console.error(error);
    }
}