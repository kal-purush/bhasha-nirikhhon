import fs from 'fs'
import path from 'path';
import { fileURLToPath } from 'url';
import { Keyboard } from 'puppeteer';
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth'
import { executablePath } from 'puppeteer';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const home = async(req,res)=>{
  res.render('home.ejs')
}
export const setupSubmit = async(req,res)=>{
    try {
        const {web_url,email,password}=req.body
        const data = JSON.stringify(req.body)
        if(fs.existsSync(__dirname+"/credentials/cred.json"))return res.status(400).json({msg:'Setup is already done',status:400})
        let write = fs.writeFileSync(__dirname+"/credentials/cred.json",data)
        // if(!write)throw("Something went wrong")
        return res.status(200).json({msg:'Setup successful',status:200})
    } catch (error) {
        console.log(error)
        return res.status(500).json({msg:'Something went wrong',status:500})
    }
}

export const voiceAsstPage = async (req,res)=>{
    res.render('voice_asst.ejs')
}

// export const voiceCommand = async(req,res)=>{
//   const {command}=req.body
//   console.log(command)

//     let read = fs.readFileSync(__dirname+"/credentials/cred.json")
//     let data = JSON.parse(read);
//     (async()=>{
//        const browser = await puppeteer.launch({
//          headless:false,
//          args:[
//             '--no-sandbox',
//             '--disable-gpu',
//             '--enable-webgl',
//             '--window-size=1920,1200'
//          ],
//          executablePath:executablePath()

//        })
//        const ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
//        const page = await browser.newPage()
//        await page.setUserAgent(ua)
//        await page.goto(data.web_url, {waitUntil:'networkidle2'})
//        await page.type('input[type="email"]',data.email)
//        await page.type('input[type="password"]',data.password)
//        await page.keyboard.press('Enter')
//        await page.waitForNavigation()
//     //    page.click("#side-menu > li:nth-child(3) > a")
//        if(command.includes('clock in')){
//         try {
//          let element = await page.$('#clock-in')
//          if(!element)throw("error") 
//          await page.click("#clock-in")
//          return res.json({msg:"clocked in"})
//         } catch (error) {
//             return res.json({msg:"either you are already clocked in or maximum clock in reached"})
//         } finally{
//             await browser.close();
//         }   
//        }

//        if(command.includes('clock out')){
//         try {
//             let element = await page.$('#clock-out') 
//             if(!element)throw("error") 
//             await page.click("#clock-out")
//             return res.json({msg:"clocked out"})
//         } catch (error) {
//             return res.json({msg:"either you are already clocked out or maximum clock in reached"})
//         } finally{
//             await browser.close();
//         }  
//        }
       

       
//     })()
// }

export const automateClockIn = async () =>{
    console.log("running")
    let read = fs.readFileSync(__dirname+"/credentials/cred.json")
    let data = JSON.parse(read);
    let command = "clock in";
    let msg="";
    (async()=>{
       const browser = await puppeteer.launch({
         headless:false,
         args:[
            '--no-sandbox',
            '--disable-gpu',
            '--enable-webgl',
            '--window-size=1920,1200'
         ],
         executablePath:executablePath()

       })
       const ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
       const page = await browser.newPage()
       await page.setUserAgent(ua)
       await page.goto(data.web_url, {waitUntil:'networkidle2'})
       await page.type('input[type="email"]',data.email)
       await page.type('input[type="password"]',data.password)
       await page.keyboard.press('Enter')
       await page.waitForNavigation()
    //    page.click("#side-menu > li:nth-child(3) > a")
       if(command.includes('clock in')){
        try {
         let element = await page.$('#clock-in')
         if(!element)throw("error") 
         await page.click("#clock-in")
         return res.json({msg:"clocked in"})
        } catch (error) {
            return msg="either you are already clocked in or maximum clock in reached";
        } finally{
            await browser.close();
        }   
       }

       if(command.includes('clock out')){
        try {
            let element = await page.$('#clock-out') 
            if(!element)throw("error") 
            await page.click("#clock-out")
            return res.json({msg:"clocked out"})
        } catch (error) {
            return msg="either you are already clocked out or maximum clock in reached";
        } finally{
            await browser.close();
        }  
       }
       

       
    })()
}