const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const VIN = require('../models/VIN');
const generateOutput = require('./generateOutput');
const scrapeDataNew = async() => {
    
    const sleep = (ms) => {return new Promise(resolve => setTimeout(resolve, ms));}
    const browser = await puppeteer.launch({ 
        headless: true,
        // args: ["--no-sandbox", "--disable-setuid-sandbox"],
        defaultViewport: {
            width: 1920,
            height: 1080
        }
    });
    try{
        const vincheck = await browser.newPage();
        await vincheck.goto('https://www.autocheck.com/members/login.do');
        await vincheck.waitForSelector('#loginform');
        const form = await vincheck.$('#loginform');
        const username = await form.$('#username');
        const password = await form.$('#password');
        const login = await form.$('input[type="submit"]');
        await username.type('5024533');
        await password.type('matthews2');
        await login.click();
        await vincheck.waitForNavigation();

        
        const vins = await VIN.findAll({where:{status: null}});
        if(vins.length !== 0){
            for(let i = 0; i < vins.length; i++){
                const values = {};
                console.log(`${vins[i].vin}: processing started`);
                const form = await vincheck.$('#singleVin');
                const vin = await form.$('#vin');
                const submit = await form.$('[name="fullButton"]');
                console.log(`${vins[i].vin}: AUTOCHECK : Typing vin`);
                await vin.type(vins[i].vin);
                console.log(`${vins[i].vin}: AUTOCHECK : Submitting vin`);
                await submit.click();
                await vincheck.waitForNavigation();
                console.log(`${vins[i].vin}: AUTOCHECK : Waiting for info page to load`);
                let accidentCount = '';
                const accidentImg = await vincheck.$('[src="https://www.autocheck.com/reportservice/report/fullReport/img/accident-found.svg"]');
                console.log(`${vins[i].vin}: AUTOCHECK : Checking for accident`);
                if(accidentImg!=null){
                    accidentCount = await accidentImg.evaluate(el=>{
                        const accidentHolder = el.parentElement.parentElement;
                        const accidentCount = accidentHolder.querySelector('.accident-count');
                        return accidentCount.innerText;
                    });
                    console.log(`${vins[i].vin}: AUTOCHECK : Accident found`);
                }else{
                    accidentCount = '0';
                }
                const invalid = await vincheck.$('#singleVin p.alert.alert-danger');
                const titleBrands = await vincheck.evaluate(async()=>{
                    const tableBody = document.querySelectorAll('.title-brand-table > .table_icon > .rTableRow');

                    let brandsCount = 0;
                    for(let i=0; i<tableBody.length; i++){
                        console.log(tableBody[i].querySelector('img').getAttribute('src')); 
                        if(tableBody[i].querySelector('img').getAttribute('src') !== 'https://www.autocheck.com/reportservice/report/fullReport/img/check-icon.svg'){
                            brandsCount++;
                        }
                    }
                    return brandsCount;
                });
                values.accident_count =invalid==null?accidentCount:'';
                values.problem_count = invalid==null?titleBrands:'';
                values.status = invalid==null?'success':'invalid';
                await VIN.update(values,{where:{vin: vins[i].vin}});
                await vincheck.goto('https://www.autocheck.com/members/singleVinSearch.do');
                await vincheck.waitForSelector('#singleVin');                  
            }

            for(let i = 0; i < vins.length; i++){
                const values = {};
                const kbb = vincheck;
                await kbb.evaluateOnNewDocument(async() => {
                    delete navigator.__proto__.webdriver;
                });
                
                let reTry = true;
                while(reTry){
                    try{
                        await kbb.setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36");
                        await kbb.goto('https://www.kbb.com/whats-my-car-worth/');
                        

                        console.log(`${vins[i].vin}: KBB : Typing vin`);
                        await kbb.waitForSelector('#vinNumberInput',{timeout:5000});
                        const vinInput = await kbb.$('#vinNumberInput input');
                        const submit = await kbb.$('button[data-testid="vinSubmitBtn"]');
                        
                        await vinInput.type(vins[i].vin);
                        console.log(`${vins[i].vin}: KBB : Submittion vin`);
                        await submit.click();
                        await sleep(1000);
                        const invalid = await kbb.$('#vinNumberInput span.css-tiv2r2-StyledError.e2plhlo1');
                        console.log(`${vins[i].vin}: KBB : Choosing  Engine & Transmission`);
                        if(invalid!=null){
                            console.log(`${vins[i].vin}: KBB : Invalid vin`);
                                values.kbb_status = 'invalid';
                                values.kbb_year= '';
                                values.kbb_vehicle= '';
                                values.kbb_engine_trim= '';
                                values.kbb_tradeInValue= '';
                                reTry = false;
                        }else{
                            console.log(`${vins[i].vin}: KBB : Choosing  category`);
                            await kbb.waitForSelector('#category',{timeout:5000});
                            const engine = await kbb.$('#engine');
                            const transmission = await kbb.$('#transmission');
                            if(await engine.$('select')!=null){
                                const select = await engine.$('select');
                                const options = await select.$$('option');
                                const firstValue = await options[1].evaluate(el=>el.value);
                                await kbb.select('#engine select', firstValue);
                            }
                            if(await transmission.$('select')!=null){
                                const select = await transmission.$('select');
                                const options = await select.$$('option');
                                const firstValue = await options[1].evaluate(el=>el.value);
                                await kbb.select('#transmission select', firstValue);
                                for(let i=1; i<options.length; i++){
                                    const value = await options[i].evaluate(el=>el.value);
                                    if(!value.includes('manual') && !value.includes('Manual')){
                                        await kbb.select('#transmission select', value);
                                        break;
                                    }
                                }
                            }

                            console.log(`${vins[i].vin}: KBB : Entering Mileage`);
                            await kbb.waitForSelector('[data-lean-auto="mileageInput"]',{timeout:5000});
                            const mileageInput = await kbb.$('[data-lean-auto="mileageInput"]');
                            const zipCodeInput = await kbb.$('[data-lean-auto="zipcodeInput"]');
                            await mileageInput.type(vins[i].mileage);
                            await zipCodeInput.type('16917');
                            await sleep(1000);
                            const firstPageNextButton = await kbb.$('button[data-cy="vinLpNext"]');
                            await firstPageNextButton.click();
                            console.log(`${vins[i].vin}: KBB : Choosing  Standard Price`);
                            await kbb.waitForSelector('#pricestandard',{timeout:5000});
                            const priceStandard = await kbb.$('#pricestandard');
                            priceStandard.click();
                            console.log(`${vins[i].vin}: KBB : Choosing  Color`);
                            await kbb.waitForSelector('[data-lean-auto="colorPicker_White"]',{timeout:5000});
                            const colorPickerWhite = await kbb.$('[data-lean-auto="colorPicker_White"]');
                            await colorPickerWhite.click();
                            const secondPageNextButton = await kbb.$('button[data-lean-auto="optionsNextButton"]');
                            await secondPageNextButton.click();
                    
                            try{
                                console.log(`${vins[i].vin}: KBB : Choosing Default Price Options`);
                                await kbb.waitForSelector('[data-lean-auto="next-btn"]',{timeout:5000});
                                const thirdPageNextButton = await kbb.$('[data-lean-auto="next-btn"]');
                                await thirdPageNextButton.click();
                            }catch(e){
                                console.log(`${vins[i].vin}: KBB : No Default Price Options`);
                            }
                            console.log(`${vins[i].vin}: KBB : Choosing condition "Good"`);
                            await kbb.waitForSelector('[data-lean-auto="good"]',{timeout:5000});
                            const good = await kbb.$('[data-lean-auto="good"]');
                            await good.click();
                            const fourthPageNextButton = await kbb.$('button[data-lean-auto="optionsNextButton"]');
                            await fourthPageNextButton.click();
                            console.log(`${vins[i].vin}: KBB : Clicking Quick Link`);
                            await kbb.waitForSelector('[data-lean-auto="quick-link"]',{timeout:5000});
                            const quickLink = await kbb.$('[data-lean-auto="quick-link"]');
                            await quickLink.click();
                            console.log(`${vins[i].vin}: KBB : waiting for trande value to appear`);
                            await kbb.waitForSelector('[name="tradeInValue"]',{timeout:5000});
                            await sleep(1000);
                            console.log(`${vins[i].vin}: KBB : Scraping Data`);
                            const tradeInValue = await (await kbb.$('[name="tradeInValue"]')).evaluate(el=>el.value);
                            const vehicleDetails = await (await kbb.$('.css-1ceovnz-HeaderAndLabel h1')).evaluate(el=>el.innerText);
                            const year = vehicleDetails.split(' ')[0];
                            const vehicle = vehicleDetails.replace(year, '').trim();
                            const engineTrim = await (await kbb.$('.css-1ceovnz-HeaderAndLabel p')).evaluate(el=>el.innerText);
                            reTry = false;
                            values.kbb_status = 'success';
                            values.kbb_year= year;
                            values.kbb_vehicle= vehicle;
                            values.kbb_engine_trim= engineTrim;
                            values.kbb_tradeInValue= tradeInValue;
                            await VIN.update(values,{where:{vin: vins[i].vin}});
                        }
                    }catch(e){
                        console.log(`${vins[i].vin}: KBB : Attempt failed. Will Try again!`);
                    }
                }
            }
        }
    }catch(err){
        console.log(err);
    }finally{
        await browser.close();
        const leftVin = await VIN.findAll({where:{status: null}});
        if(leftVin.length == 0){
            await generateOutput();
        }
    }

};
module.exports = scrapeDataNew;