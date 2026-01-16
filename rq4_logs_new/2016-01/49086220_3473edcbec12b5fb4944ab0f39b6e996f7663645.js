			function Sprite(){
				this.x = 130;
				this.y = 130;
				this.vx = 0;
				this.vy = 0;
				this.ax = 0;
				this.ay = 0;
				this.w = 32;
				this.h = 32;
				this.velocidade = 0;
				this.angulo = 0;
				this.va = 0;
				this.tiro = false;
				this.intervaloTiro = 0;
				this.image = new Image();
				this.image.src = "p1.png";
				this.vida = 10;

				this.mover = function(dt, g){
					this.angulo = this.angulo + this.va*dt;
					this.vx = Math.cos(this.angulo)*this.velocidade;
					this.vy = Math.sin(this.angulo)*this.velocidade;

					this.x = this.x + this.vx*dt;
					
					this.y = this.y + this.vy*dt;
				};
				this.desenhar = function(ctx){
						ctx.save();

						ctx.strokeStyle = "black";
						ctx.fillStyle = "rgb(250, 150, 150)";

						ctx.translate(this.x, this.y);
						ctx.rotate(this.angulo+Math.PI/2);
						ctx.drawImage(this.image, 0, 0, 32, 32,-this.w/2, -this.h/2, this.w, this.h);
						ctx.restore();

				};
				this.controlar = function(){

				};
				this.tomouChumbo = function(tiro){
					if(this.vida <= 0) return false;
					if(this.x 		 > tiro.x + tiro.raio) return false;
					if(this.x+this.w < tiro.x - tiro.raio) return false;
					if(this.y 		 > tiro.y + tiro.raio) return false;
					if(this.y+this.h < tiro.y - tiro.raio) return false;
					this.vida --;
					return true;
				};
			}

			function SpriteTiro(xi,yi,a){
				this.x = xi;
				this.y = yi;
				this.angulo = a;
				this.raio = 3;
				this.velocidade = 180;

				this.vx = 0;
				this.vy = 0;

				this.mover = function(dt){
					this.vx = Math.cos(this.angulo)*this.velocidade;
					this.vy = Math.sin(this.angulo)*this.velocidade;
				
					this.x = this.x + this.vx*dt;
					
					this.y = this.y + this.vy*dt;
				};

				this.desenhar = function(ctx){
					ctx.save();

					ctx.strokeStyle = "black";
					ctx.fillStyle = "rgb(250, 150, 150)";

					ctx.translate(this.x, this.y);
					ctx.beginPath();
					    ctx.arc(0,0, this.raio, 0, Math.PI*2, true);
				    ctx.closePath();
					ctx.fill();
					ctx.stroke();
					ctx.restore();
				};

			}

			function SpriteObstaculo(xi,yi,w,h){
				this.x = xi;
				this.y = yi;
				this.h = h;
				this.w = w;
				this.cor = "red";

				this.desenhar = function(ctx){
                        ctx.fillStyle = this.cor;
                        ctx.fillRect(this.x,this.y,this.w,this.h);
				};
				this.tomouChumbo = function(tiro){
					if(this.vida <= 0) return false;
					if(this.x 		 > tiro.x + tiro.raio) return false;
					if(this.x+this.w < tiro.x - tiro.raio) return false;
					if(this.y 		 > tiro.y + tiro.raio) return false;
					if(this.y+this.h < tiro.y - tiro.raio) return false;
					this.vida --;
					return true;
				};
				this.bateu = function(quadrado){
					if(this.x 		 > quadrado.x + quadrado.w/2) return false;
					if(this.x+this.w < quadrado.x - quadrado.w/2) return false;
					if(this.y 		 > quadrado.y + quadrado.h/2) return false;
					if(this.y+this.h < quadrado.y - quadrado.h/2) return false;
					return true;
				}

			}
