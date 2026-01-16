// import { xhr } from "winjs"
import UI from "./ui.js"

export default class Ajax {



    static getFromSite(url){
        let xhr = new XMLHttpRequest
        xhr.open('GET',url)
        xhr.onload = function(){
            let ObjCountrys= JSON.parse(this.responseText)
            let html = UI.convertToHtmlTable(ObjCountrys)
            UI.showTable(html)
        }
        xhr.send()


    }
}