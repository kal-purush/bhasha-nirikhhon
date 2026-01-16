// EXTRAINDO DADOS

const puppeteer = require('puppeteer'); //Chamando biblioteca
const urlalvo = 'https://rj.olx.com.br/norte-do-estado-do-rio/imoveis/sobrado-04-quartos-um-suite-parque-joao-maria-929678008?lis=listing_1001';

let detalhesImovel = [];

const wspg = async() => {
    const browser = await puppeteer.launch({
        headless: false, // Só fica ativo no ambiente de de homologação
    });
    const page = await browser.newPage();

    await page.goto(urlalvo);
    await page.waitForTimeout(3000);
    const nome = await page.$eval('.ad__sc-45jt43-0.fAoUhe.sc-cooIXK.kMRyJF', (el) => el.textContent);
    const valor = await page.$eval('.ad__sc-1wimjbb-1.hoHpcC.sc-cooIXK.cXlgiS', (el) => el.textContent);
    const codigobruto = await page.$eval('.ad__sc-16iz3i7-0.bTSFxO.sc-ifAKCX.   fizSrB', (el) => el.textContent);
    const codigo = codigobruto.replace(/\D/gim, ''); //Limpa string de código deixando apenas números.

    const urlimovel = urlalvo;
    const detalhesArray = await page.$$('.ad__h3us20-4.cxrGoW');

    for(let detalhesElement of detalhesArray){
        let detalhesImv = await detalhesElement.$eval('.ad__duvuxf-0.ad__h3us20-0.kUfvdA', element => element.innerText);
        const detalhes = await detalhesImv.replace('\n', ' : ');
        detalhesImovel.push(detalhes)
    }

    await console.log({nome, valor, urlimovel, codigo, detalhesImovel});

    await browser.close();



}
wspg();



// page.$eval - Equivale ao document.querySelect
// page.$$ - document.querySelect