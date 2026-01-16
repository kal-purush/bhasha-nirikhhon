//set up day and night effect
export function updatedayandnightEffetc(ctx: CanvasRenderingContext2D, canvas: HTMLCanvasElement): void {
    const hour = new Date().getHours();
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    if (hour >= 6 && hour < 18) {
        // Daytime
        ctx.fillStyle = "rgba(135, 206, 235, 0.5)"; // Light blue for daytime
    } else {
        // Nighttime
        ctx.fillStyle = "rgba(25, 25, 112, 0.5)"; // Dark blue for nighttime
    }
    ctx.fillRect(0, 0, canvas.width, canvas.height);

}

// and rain effect for the game     
export function createRainEffect(ctx: CanvasRenderingContext2D, raindrop: any[], canvas: HTMLCanvasElement): void {
        if(raindrop.length === 0){
            for(let i=0;i< 100;i++){
                raindrop.push({
                    x: Math.random() * canvas.width,
                    y: Math.random() * canvas.height,
                    length: Math.random() * 20 + 10,
                    speed: Math.random() * 5 + 2
                });
            }
        }

        //draw raindrop
        ctx.strokeStyle = '#87CEFA';
        ctx.lineWidth = 2;
        for(const drop of raindrop){
            ctx.beginPath();
            ctx.moveTo(drop.x, drop.y);
            ctx.lineTo(drop.x, drop.y + drop.length);
            ctx.stroke();

            //update raindrop position
            drop.y += drop.speed;
            if(drop.y > canvas.height){
                drop.y = 0;
                drop.x = Math.random() * canvas.width;
            }
        }

}