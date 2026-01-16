
class Graph {
    INNER_OFFSET = 50;
    
    TEXT_OFFSET = -5;
    TEXT_HEIGHT = 18;
    TEXT_SIDE_OFFSET = 8;
    
    CANVAS_LINES = '#222222';
    BG_CANVAS_LINES = '#555555';
    TICK_WIDTH = 5;

    graphs = [];

    globalMin = 0;
    globalMax = 0;

    maxDataLen = 0;

    addDataGraph(dat, gColor) {
        let minmax = this.getMinMax(dat);

        if (this.graphs.length == 0) {
            this.globalMin = minmax[0];
            this.globalMax = minmax[1];
        } else {
            this.globalMin = Math.min(minmax[0], this.globalMin);
            this.globalMax = Math.max(minmax[1], this.globalMax);
        }

        this.graphs[this.graphs.length] = {
            min: minmax[0],
            max: minmax[1],
            data: dat,
            color: gColor
        }

        this.maxDataLen = Math.max(this.maxDataLen, dat.length); 

        return this.graphs.length-1;
    }

    getMinMax(data) {

        if (data.length == 0) {
            return [0, 0];
        }

        let cMax = data[0], cMin = data[0];

        for (let cI = 0; cI < data.length; cI++) {
            if (data[cI] < cMin) cMin = data[cI];
            if (data[cI] > cMax) cmax = data[cI];
        }
        return [cMin, cMax];
    }
    
    appendData(newDat, graphI) {
        if (graphI >= this.graphs.length) return undefined;
        
        if (this.graphs[graphI].data.length == 0) {
            this.graphs[graphI].min = newDat;
            this.graphs[graphI].max = newDat;
        } else {
            if (newDat < this.graphs[graphI].min) this.graphs[graphI].min = newDat;
            if (newDat > this.graphs[graphI].max) this.graphs[graphI].max = newDat;
        }

        if (newDat < this.globalMin) this.globalMin = newDat;
        if (newDat > this.globalMax) this.globalMax = newDat;
        
        this.graphs[graphI].data[this.graphs[graphI].data.length] = newDat;

        //console.log('Graph Data: ', this.graphs[graphI]);
        this.maxDataLen = Math.max(this.maxDataLen, this.graphs[graphI].data.length);
    }
    getPxFromDat(dat, min, max, minPx, maxPx) {
        if (dat > max) return maxPx;
        if (dat < min) return minPx;
        return Math.round(((maxPx-minPx) * dat)/(max-min));
    }

    getZeroPxOffs(starty, endy) {
        return this.globalMin < 0 ? -((endy-starty)*this.globalMin)/(this.globalMax - this.globalMin) : 0;
    }


    draw(ctx, pxWidth, pxHeight) {
        //console.log('graphs: ', pxWidth, pxHeight);
        this.drawCanvas(ctx, pxWidth, pxHeight);

        for (let cGraphI = 0; cGraphI < this.graphs.length; cGraphI++) {
            this.drawGraph(ctx, pxWidth, pxHeight, cGraphI);
        }
    }

    drawGraph(ctx, pxWidth, pxHeight, cGI) {
        if (cGI >= this.graphs.length || cGI < 0) return;
        
        let cG = this.graphs[cGI];
        
        const startx = this.INNER_OFFSET, endx = pxWidth - this.INNER_OFFSET;
        const starty = this.INNER_OFFSET, endy = pxHeight - this.INNER_OFFSET;

        ctx.fillStyle = null;
        ctx.strokeStyle = cG.color;

        let x = startx;
        let y = endy - (((endy-starty) * cG.data[0])/(this.globalMax-this.globalMin)) - this.getZeroPxOffs(starty, endy);
        ctx.beginPath();
        ctx.moveTo(x, y) ;
        for (let c = 0; c < cG.data.length; c++) {
            //console.log(startx + this.getPxFromDat(c, 0, this.maxDataLen, startx, endx));     
            //ctx.lineTo(startx + this.getPxFromDat(c, 0, this.maxDataLen, startx, endx), endy - this.getPxFromDat(cG.data[c], this.globalMin, this.globalMin, starty, endy));
            x = startx + (((endx-startx) * c)/cG.data.length);
            y = endy - (((endy-starty) * cG.data[c])/(this.globalMax-this.globalMin)) - this.getZeroPxOffs(starty, endy);
            ctx.lineTo(x, y);
        }
        ctx.stroke();
    }

    drawCanvas(ctx, pxWidth, pxHeight) {
        //console.log('Got Here! ', pxWidth, pxHeight, ctx);


        // draw bounding box with tiks
        const startx = this.INNER_OFFSET, endx = pxWidth - this.INNER_OFFSET;
        const starty = this.INNER_OFFSET, endy = pxHeight - this.INNER_OFFSET;
        const deltax = endx - startx, deltay = endy - starty;
        
        ctx.fillStyle = null;

        let workingY = Math.round(deltay/2), workingX = Math.round(deltax/2);
        ctx.strokeStyle = this.CANVAS_LINES;
        // Tik on y-axis
        ctx.beginPath();
        ctx.moveTo(startx - this.TICK_WIDTH, starty + workingY);
        ctx.lineTo(startx, starty + workingY);
        
        // Tik on x-axis
        ctx.moveTo(startx + workingX, endy + this.TICK_WIDTH);
        ctx.lineTo(startx + workingX, endy);
        ctx.closePath();
        ctx.stroke();

        // Inner half line y
        ctx.strokeStyle = this.BG_CANVAS_LINES;
        ctx.beginPath();
        ctx.moveTo(startx, starty + workingY);
        ctx.lineTo(endx, starty + workingY);
        ctx.moveTo(startx + workingX, starty);
        ctx.lineTo(startx + workingX, endy);
        ctx.closePath();
        ctx.stroke();

        // Zero line
        let zeroOffs = this.getZeroPxOffs(starty, endy);
        //console.log('deltay: ', deltay, 'zeroffs: ', zeroOffs);
        if (zeroOffs !== 0) {
            ctx.strokeStyle = this.CANVAS_LINES;
            ctx.beginPath();

            ctx.moveTo(startx, endy - zeroOffs);
            ctx.lineTo(endx, endy - zeroOffs);
            ctx.stroke();
        }
        // zero Num
        ctx.font = this.TEXT_HEIGHT + 'px sans-serif';
        ctx.fillStyle = this.CANVAS_LINES;
        ctx.fillText('0', this.TEXT_SIDE_OFFSET, endy - this.TEXT_OFFSET - zeroOffs);
        let cI = 0;
        // Min/ Max Value
        if (zeroOffs > this.TEXT_HEIGHT) {
            cI = Math.round((this.globalMin + Number.EPSILON) * 100) / 100;
            ctx.fillText(cI.toString(), this.TEXT_SIDE_OFFSET, endy - this.TEXT_OFFSET);
        }
        if (Math.abs(deltay - zeroOffs) > this.TEXT_OFFSET) {
            cI = Math.round((this.globalMax + Number.EPSILON) * 100) / 100;
            ctx.fillText(cI.toString(), this.TEXT_SIDE_OFFSET, starty - this.TEXT_OFFSET);
        }
        
        ctx.fillText(this.maxDataLen.toString(), endx, endy + this.TEXT_HEIGHT);
    

        //Bounding Box
        ctx.strokeStyle = this.CANVAS_LINES;
        ctx.fillStyle = null;
        ctx.beginPath();
        ctx.strokeRect(startx, starty, deltax, deltay);


        ctx.strokeStyle = this.CANVAS_LINES;
    }
}