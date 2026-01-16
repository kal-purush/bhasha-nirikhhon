const szabbutton = document.getElementById("szabalyzat")
const startgame = document.querySelector("#startgame")
const nyitooldal = document.getElementById("nyitooldal")
const playernumber = document.getElementById("playernumber")
const jatekosnevekdiv= document.getElementById("jatekosnevek")
const game = document.getElementById("game")
const kartyak=document.getElementById("kartyak")
const selected=document.getElementById("selectedcards")
const selectedThree=document.getElementById("selectedthree")

let isSet=true

const cards = 
[
    {
        color:"red",
        number: 1,
        shape:"rombusz",
        src:"(1).png"
    },
    {
        color:"red",
        number: 2,
        shape:"rombusz",
        src:"(2).png"
    },
    {
        color:"red",
        number: 3,
        shape:"rombusz",
        src:"(3).png"
    },
    {
        color:"red",
        number: 1,
        shape:"line",
        src:"(4).png"
    },
    {
        color:"red",
        number: 2,
        shape:"line",
        src:"(5).png"
    },
    {
        color:"red",
        number: 3,
        shape:"line",
        src:"(6).png"
    },
    {
        color:"red",
        number: 1,
        color:"wavey",
        src:"(7).png"
    },
    {
        color:"red",
        number: 2,
        shape:"wavey",
        src:"(8).png"
    },
    {
        color:"red",
        number: 3,
        shape:"wavey",
        src:"(9).png"
    },
    {
        color:"green",
        number: 1,
        shape:"rombusz",
        src:"(10).png"
    },
    {
        color:"green",
        number: 1,
        shape:"line",
        src:"(11).png"
    },
    {
        color:"green",
        number: 1,
        shape:"wavey",
        src:"(12).png"
    },
    {
        color:"green",
        number: 2,
        shape:"rombusz",
        src:"(13).png"
    },
    {
        color:"green",
        number: 2,
        shape:"line",
        src:"(14).png"
    },
    {
        color:"green",
        number: 2,
        shape:"wavey",
        src:"(15).png"
    },
    {
        color:"green",
        number: 3,
        shape:"rombusz",
        src:"(16).png"
    },
    {
        color:"green",
        number: 3,
        shape:"line",
        src:"(17).png"
    },
    {
        color:"green",
        number: 3,
        shape:"wavey",
        src:"(18).png"
    },
    {
        color:"purple",
        number: 1,
        shape:"rombusz",
        src:"(19).png"
    },
    {
        color:"purple",
        number: 1,
        shape:"line",
        src:"(20).png"
    },
    {
        color:"purple",
        number: 1,
        shape:"wavey",
        src:"(21).png"
    },
    {
        color:"purple",
        number: 2,
        shape:"rombusz",
        src:"(22).png"
    },
    {
        color:"purple",
        number: 2,
        shape:"line",
        src:"(23).png"
    },
    {
        color:"purple",
        number: 2,
        shape:"wavey",
        src:"(24).png"
    },
    {
        color:"purple",
        number: 3,
        shape:"rombusz",
        src:"(25).png"
    },
    {
        color:"purple",
        number: 3,
        shape:"line",
        src:"(26).png"
    },
    {
        color:"purple",
        number: 3,
        shape:"wavey",
        src:"(27).png"
    }
]
const players=[]
function genPlayers()
{
    const allplayers = document.getElementById("allplayers")
    for(let idx=0; idx<jatekosnevekdiv.childElementCount;idx++)
    {players.push(
        {
        name : jatekosnevekdiv.children[idx].value,
        score : 0
        } 
        )}

    for(let idx=0; idx<players.length;idx++)
    {
        const button = document.createElement("button")
        button.textContent = players[idx].name
        allplayers.appendChild(button)

    }
    
}

function randBtw(a,b){
    return Math.floor(Math.random() * b) + a
}

szabbutton.addEventListener('click',function(e){
    if(document.getElementById("szabalyok").style.visibility=="visible")
    {
        document.getElementById("szabalyok").style.visibility="hidden";
    }
    else
    {
        document.getElementById("szabalyok").style.visibility="visible";
    }
})

playernumber.addEventListener('click',insertJatekosNevek)

function insertJatekosNevek()
{
    for (let index = jatekosnevekdiv.childElementCount; jatekosnevekdiv.childElementCount > playernumber.value ; index--) {
        jatekosnevekdiv.lastChild.remove();
    }
    for(let idx=jatekosnevekdiv.childElementCount; idx<playernumber.value;idx++)
    {
       const tmp = document.createElement("input")
       tmp.type = "text"
       tmp.value = `Játékos${idx+1}`
       jatekosnevekdiv.appendChild(tmp);
    }
   
}


startgame.addEventListener('click',function(e){
    nyitooldal.style.visibility="hidden"

    game.style.visibility="visible"
    for(let idx=0;idx<4; idx++)
    {
        const tmp = document.createElement("tr")
        
        for(let idx=0; idx<3;idx++)
        {
            const randnumb = randBtw(1,27)
            const tmptd = document.createElement("td")
            const myimg= document.createElement("img")
            myimg.src=`clear/   (${randnumb}).png`
            myimg.addEventListener('click',selectCard)
            tmptd.appendChild(myimg)
            tmp.appendChild(tmptd)
            
        }
        kartyak.appendChild(tmp)
    }
    genPlayers()
})
let idx = null
function selectCard(e)
{
    idx=0
    while(selectedThree.cells[idx]!=null && idx < selectedThree.cells.length && selectedThree.cells[idx].childElementCount!=0 )
    {++idx;}
    const myimg= document.createElement("img")
    myimg.src=e.target.src
    if(selectedThree.cells[idx]!=null)
    {selectedThree.cells[idx].appendChild(myimg)}
    
    if(idx==3 || idx == 2 )
    {
        decideIfSet()
        console.log(0)
    }
}

let elso = null
let masodik = null
let harmadik = null

function decideIfSet()
{
    for(let idx=0; idx<cards.length;idx++)
    {
        if(selectedThree.cells[0].firstChild.src.split("%20%20%20")[1]==cards[idx].src)
        {
            elso=cards[idx]
        }
        if(selectedThree.cells[1].firstChild.src.split("%20%20%20")[1]==cards[idx].src)
        {
            masodik=cards[idx]
        }
        if(selectedThree.cells[2].firstChild.src.split("%20%20%20")[1]==cards[idx].src)
        {
            harmadik=cards[idx]
        }
    }

    isSet = ((elso.color == masodik.color && elso.color == harmadik.color && masodik.color == harmadik.color) || (elso.color != masodik.color && elso.color != harmadik.color && masodik.color != harmadik.color))       &&
            ((elso.number == masodik.number && elso.number == harmadik.number  && masodik.number == harmadik.number) || (elso.number != masodik.number && elso.number != harmadik.number && masodik.number != harmadik.number))  &&
            ((elso.shape == masodik.shape && elso.shape == harmadik.shape && masodik.shape == harmadik.shape) || (elso.shape != masodik.shape && elso.shape != harmadik.shape && masodik.shape != harmadik.shape))
    console.log(isSet)
}