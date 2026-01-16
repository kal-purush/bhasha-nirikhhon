const productsDiv = document.getElementById('products')

window.onload = getAllProducts

 async function getAllProducts(){
    const adress = 'product.json'

    try {
        const oxunmayanData = await fetch(adress)
        const oxunanData =  await oxunmayanData.json()

        for(let i = 0; i<oxunanData.length; i++){

            productsDiv.innerHTML += `
            <div class="info">
            <div class="left">
            <div class="header">
            <h1>${oxunanData[i].title}</h1>
            </div>
            <div class="top">
            <img src="${oxunanData[i].url}" /> 
            <img src="${oxunanData[i].url2}" />
            </div>
            <div class="bottom">
            <img src="${oxunanData[i].url3}" /> 
            <img src="${oxunanData[i].url4}" /> 
            </div>
            </div>
            <div class="right">
            <div class="right-info">
            <p class="information">${oxunanData[i].info}</p>
            <p>${oxunanData[i].motor}</p>
            <p>${oxunanData[i].speed}</p>
            <p>${oxunanData[i].ofer}</p>
            </div>
            <div class="button">
            <button> ${oxunanData[i].price}</button>
            </div>
            </div>
            </div>
            
            `
        }
    }

    catch (problem) {
        console.log(`xəta firladildi: ${problem} `)
    }
    
}