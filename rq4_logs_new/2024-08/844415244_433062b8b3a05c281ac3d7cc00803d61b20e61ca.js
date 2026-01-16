
const apiUrl = 'https://api.tinyurl.com/dev/api-create.php?url=';
const inputField = document.getElementById('urlInput'); 
const shortenButton = document.getElementById('shortenButton'); 
shortenButton.addEventListener('click', async () => {
    const longUrl = inputField.value;
    try {
        const response = await fetch(apiUrl + encodeURIComponent(longUrl));
        const shortUrl = await response.text();
        
        console.log(response);
        document.getElementById('shortenedUrl').innerHTML = shortUrl;

      //  console.log('Shortened URL:', shortUrl);
    } catch (error) {
        console.error('Error:', error);
    }
});