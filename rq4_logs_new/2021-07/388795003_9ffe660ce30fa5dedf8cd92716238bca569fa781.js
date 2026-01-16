class Food{

    constructor(){
        this.milkImage=loadImage("images/Milk.png")
        this.feedTime
        this.feedTimeMin
        this.addTime
        this.addTimeMin
    }

    buttons(){

        this.button1=createButton("feed the dog")
        this.button1.position(700,80)

        this.button2=createButton("add feed")
        this.button2.position(800,80)

        if (food>0){


        this.button1.mousePressed(()=>{

            food--
            WriteStock(food)
        })

    }

        this.button2.mousePressed(()=>{

            food++
            WriteStock2(food)
        })
    }
    

 
    milkImg(){

        var posX=10
    

        for (var i=0;i<food;i++){
            image(this.milkImage,posX,150,50,50)
            posX=posX+50

        }
    }

    getFeedTime(){
            database.ref('feedTime').on("value",(data)=>{

                this.feedTime=data.val()
            })

    }

    getFeedTimeMin(){

        database.ref('feedTimeMin').on("value",(data)=>{
            this.feedTimeMin=data.val()
        
        })
    }

    getAddTime(){
        database.ref('addTime').on("value",(data)=>{
            this.addTime=data.val()
        })
    }

     getAddTimeMin(){
        database.ref('addTimeMin').on("value",(data)=>{
                this.addTimeMin=data.val()
            })
    }


    updateState(state){
        database.ref('/').update({
            gameState:state
        })
    }

    bedRoom(){
        background(bedRoomImg)
    }
    washRoom(){
        background(washRoomImg)
    }
    garden(){
        background(gardenImg)
    }
}