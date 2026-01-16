//Single Responsobility Principle

//Принцип ответственности одного объекта

class New {
    constructor(tittle, text){
        this.tittle = tittle;
        this.text = text;
        this.modified = false;
    }

    update(text){
        this.text = text;
        this.modified = true;
    }

    //!нарушает принцип - ответственность новости нарушена. Новость не должна отвечать за вывод в html
    // toHTML(){ 
    //     return `
    //         <div class="news">
    //             <h1>${this.tittle}</h1>
    //             <p>${this.text}</p>
    //         </div>
    //     `
    // }
}

const news = new New('Путин', 'Новая конституция');

//!нарушаем принцип - ответственность новости нарушена. Новость не должна отвечать за вывод в html
//console.log(news.toHTML());

//*Для вывода информации о новости создадим отдельный класс

class NewsPrinter{
    constructor(news){
        this.news = news;
    }

    html(){
        return `
            <div class="news">
                <h1>${this.news.tittle}</h1>
                <p>${this.news.text}</p>
            </div>
        `
    }
    json(){
        return JSON.stringify({
            tutle: this.news.tittle,
            text: this.news.text,
            modified: this.news.modified
        },null, 2);
    }

    xml(){
        return `
            <news>
                <title>${this.news.tittle}</tittle>
                <text>${this.news.text}</text>
                <modified>${this.news.modified}</modified>
            </news>
        `
    }
}

const printNews = new NewsPrinter(
    new New ('Путин', 'Новая конституция')
)

console.log(printNews.json());
console.log(printNews.html());
console.log(printNews.xml());