class Sea {
    constructor(x, y) {
        this.x = x;
        this.y = y;
       
        this.directions = [
            [this.x - 1, this.y - 1],
            [this.x, this.y - 1],
            [this.x + 1, this.y - 1],
            [this.x - 1, this.y],
            [this.x + 1, this.y],
            [this.x - 1, this.y + 1],
            [this.x, this.y + 1],
            [this.x + 1, this.y + 1]
        ];
       

    }
    chooseCell(character,matrix) {
        var found = [];
        for (var i in this.directions) {
            var x = this.directions[i][0];
            var y = this.directions[i][1];
            if (x >= 0 && x < matrix[0].length && y >= 0 && y < matrix.length) {
                if (matrix[y][x] == character) {
                    found.push(this.directions[i]);
                }
            }
        }
        return found;
    }


    
    mul(matrix,seaArr,time,summer) {   
   
    
    if(time ==15 && summer){
        for(var i=0; i<=15;i++){
        var seaCord = this.chooseCell(i, matrix);
        var coord =seaCord[Math.floor(Math.random() * seaCord.length)];
            
       
        if(coord && seaArr.length <300){ 
            var x = coord[0];
            var y = coord[1];
            var newsea= new Sea(x, y);
            seaArr.push(newsea);  
            //console.log(matrix[y])
            matrix[y][x] = 4;   
           // console.log("sea")
            }
        }
        }
    
    else if(time >= 24&& seaArr.length>50 || !summer && seaArr.length>50){
            if(1 ==Math.floor(Math.random() * 10 )){
            for( var o in seaArr){
                if(seaArr[o].x==this.x&& seaArr[o].y==this.y){
                    matrix[this.y][this.x] = 0;   
                    seaArr.slice(i,1);
                    ///console.log("sea")
                    
                }
            }
        }
           
            //console.log( x ,y)
          
                
        
           
        }
       
    }
}


module.exports = Sea;