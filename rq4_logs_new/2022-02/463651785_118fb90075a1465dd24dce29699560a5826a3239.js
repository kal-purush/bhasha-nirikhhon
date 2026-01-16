// <--utility methods for address handling -->
const GLOBAL_API_KEY = 'AIzaSyAOZYe-b1-WzNvMSyleHk8AUGUUI6id5Pk';

export async function getCoordsFromAddress (address) {
    const addressURI = encodeURI(address); //included in the browser, converts address into readable coordinates
    const response = await fetch(`https://maps.googleapis.com/maps/api/geocode/json?address=${addressURI}&key=${GLOBAL_API_KEY}`); //Or use Axios
        if(!response.ok) {
            throw new Error('Failed to catch coordinates. Please try again');
        }
        const data = await response.json(); //parses it into javascript readable language
        if (data.error_message) { //if the error_message property of response exists it means there's an error
            throw new Error(data.error.message);
        }
        console.log(data);
        const coordinates = data.results[0].geometry.location; //the coordinates are in the first element of the results array which is an object. Inside that object we find the coordinates nested inside the locaiton object which is nested inside the geometry object
        return coordinates;
    }; 