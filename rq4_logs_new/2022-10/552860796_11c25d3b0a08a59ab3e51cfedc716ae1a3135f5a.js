class GKS {
    w;
    h;
    sX;
    sY;
    originX;
	originY;
    g;
    #mt2d = null;

    constructor(platno, xMin, xMax, yMin, yMax)
    {
		if(!platno) alert("Greška - nema platna!");

        this.w = platno.width;
        this.h = platno.height
        this.platno = platno

        if(yMin == null || yMax == null)
        {
            this.xMin = xMin
            this.xMax = xMax

            this.sX = this.w/(xMax - xMin)
            this.sY = this.sX       

            this.yMin = -(this.h/this.sY)/2
            this.yMax = (this.h/this.sY)/2
        }
        else if(xMin == null || xMax == null)
        {
            this.yMin = yMin
            this.yMax = yMax

            this.sY = this.h/(yMax - yMin)
            this.sX = this.sY       

            this.xMin = -(this.w/this.sX)/2
            this.xMax = (this.w/this.sX)/2
        }
        else{
            this.xMin = xMin
            this.xMax = xMax
            this.yMin = yMin
            this.yMax = yMax

            this.sX = this.w/(xMax - xMin)
            this.sY = this.h/(yMax - yMin)
        }

        this.originX = this.sX * Math.abs(this.xMin)
        this.originY = this.h - this.sY * Math.abs(this.yMin)
        this.slikajKoordinatniSustav()

        this.g.beginPath()
    }


    slikajKoordinatniSustav() {
        this.g = this.platno.getContext("2d");

        //pozadina
        this.g.fillStyle = "#FFFFE0";
        this.g.fillRect(0, 0, this.w, this.h);

        //crtanje linije
        this.g.beginPath();
        
        // x with numbers
        let isiFloat;
        this.g.moveTo(0, this.originY);
        for(let i = this.xMin, step = 0; i < this.xMax; ){
            
            isiFloat = Math.round(i) != i

            if(!isiFloat)
            {
                this.g.moveTo(step, this.originY - (this.h*0.01))
                this.g.lineTo(step,this.originY + (this.h*0.01))
            }
            this.g.moveTo(step,this.originY)
            this.g.lineTo(step+this.sX,this.originY)

            if(i != 0 && !isiFloat)
            {
                this.g.font = "1em Arial";
                this.g.fillStyle = "black"
                this.g.textAlign = 'center'
                this.g.fillText(Math.round(i),step,this.originY - (this.h*0.02))
            }
            
            if(isiFloat)
            {
                let excess = Math.abs(Math.round(i) - i)
                i += excess
                step +=this.sX*excess
            }
            else{
                i++
                step+=this.sX
            }
        }
        
        // y with numbers
        this.g.moveTo(this.originX, 0);
        for(let i = this.yMax, step = 0; i > this.yMin; ){

            isiFloat = Math.round(i) != i
            if(!isiFloat)
            {
                this.g.moveTo(this.originX  -(this.h*0.01), step)
                this.g.lineTo(this.originX + (this.h*0.01), step)
            }

            this.g.moveTo(this.originX, step)
            this.g.lineTo(this.originX, step+this.sY)

            if(i != 0 && !isiFloat){
                this.g.font = "1em Arial";
                this.g.fillStyle = "black"
                this.g.textAlign = 'center'
                this.g.textBaseline = 'middle'
                this.g.fillText(i,this.originX + (this.w*0.03), step)
            } 
                
            if(isiFloat)
            {
                let excess = Math.abs(Math.round(i) - i)
                i -= excess
                step +=this.sY*excess
            }
            else{
                i--
                step+=this.sY
            }
        }

        this.g.strokeStyle = "black";
        this.g.stroke();
    }

    pocniPut()
    {
        this.g.beginPath()
    }
    postaviNa(x,y)
    {
        let xPix = this.originX + x*this.sX
        let yPix = this.originY - y*this.sY
        this.g.moveTo(xPix, yPix);
    }
    linijaDo(x,y)
    {
        let xCrta
        let yCrta

        if(this.#mt2d != null){
            // xCrta = this.#mt2d.matrica[0][0]*x + this.#mt2d.matrica[0][1]*y + this.#mt2d.matrica[0][2]
            // yCrta = this.#mt2d.matrica[1][0]*x + this.#mt2d.matrica[1][1]*y + this.#mt2d.matrica[1][2]
            xCrta = this.#mt2d.matrix[0][0]*x + this.#mt2d.matrix[0][1]*y + this.#mt2d.matrix[0][2]
            yCrta = this.#mt2d.matrix[1][0]*x + this.#mt2d.matrix[1][1]*y + this.#mt2d.matrix[1][2]
        } 
        else{
            xCrta = x;
            yCrta = y;
        }

        let xPix = this.originX + xCrta*this.sX
        let yPix = this.originY - yCrta*this.sY
        this.g.lineTo(xPix, yPix);
    }
    koristiBoju(boja)
    {
        this.g.strokeStyle = boja
    }
    povuciLiniju()
    {
        this.g.stroke()
    }

    trans(m){

        this.#mt2d = m
        
    }

}