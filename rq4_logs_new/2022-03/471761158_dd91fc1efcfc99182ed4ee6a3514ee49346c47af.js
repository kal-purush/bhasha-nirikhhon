async function traduzir(termo){
    termo = "\"" + termo + "\""

    const options = {
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            'X-RapidAPI-Host': 'microsoft-translator-text.p.rapidapi.com',
            'X-RapidAPI-Key': 'c8e094152dmsh71e9f5939ffa21ep187c77jsn5fb30a9dec81'
        },
        body: `[{"Text":${termo}}]`
    };
    
    let traducao = await fetch('https://microsoft-translator-text.p.rapidapi.com/translate?to=pt&api-version=3.0&from=en&profanityAction=NoAction&textType=plain', options)
    .then(response => response.json())
    .then(response => {
        //console.log(response[0].translations[0].text)
        return response[0].translations[0].text
    })
    .catch(err => console.error(err));
    
    //console.log(traducao)
    return traducao

}

export {traduzir}