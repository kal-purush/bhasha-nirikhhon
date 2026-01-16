var shape = {
    name: "rectangle",
    popup: function (){
        console.log('this inside popup():' + this.name);
setTimeout(()=>{
            console.log('this inside setTimeout():' + this.name);
            console.log('I am a: '+ this.name);
        }, 3000);
    }
}

shape.popup();