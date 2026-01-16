(() => {
	const VERSION = '1.1.0';
	console.log('runai version', VERSION);
	class RunAI {
		draw() {
			if (!this.opts.draw) return;
			var ctx = this.runner.canvasCtx;
			var rexX = this.rex.xPos + 20;
			var rexY = this.rex.yPos + 30;

			// Render lines to obsticles
			ctx.lineWidth = 3;
			ctx.strokeStyle = 'red';
			ctx.fillStyle = 'red';
			for (var i = 0; i < this.hor.obstacles.length; ++i) {
				// Calculate line positions
				var o = this.hor.obstacles[i];
				var ox = this.hor.obstacles[i].xPos;
				var oy = this.hor.obstacles[i].yPos;
				var ow = this.hor.obstacles[i].width;
				var oh = this.hor.obstacles[i].size * 15;
				var endX = ox + (ow / 2);
				var endY = oy + (oh / 2);
				var cactusName = RunAI.cactusMap[o.typeConfig.type];
				// Render line
				ctx.save();
				ctx.lineWidth = 1.5;
				ctx.beginPath();
				ctx.moveTo(rexX, rexY);
				ctx.lineTo(endX, endY);
				ctx.stroke();
				ctx.restore();
				// Render debug text about obsticles
				ctx.font = '12px Ubuntu';
				var txt = 'Distance (' +
					(Math.max(Math.ceil((ox - this.rex.xPos) * 100) / 100, 2.8)) + ", " + (oy - this.rex.yPos) +
					") / size " + (o.size);
				var fontX = endX - (txt.length * 2);
				var fontY = endY - 25;
				ctx.fillText(txt, fontX, fontY);
				// Render collision boxes
				ctx.lineWidth = 1;
				for (var j = 0; j < o.collisionBoxes.length; ++j) {
					var box = o.collisionBoxes[j];
					var x = ox + box.x,
						y = oy + box.y,
						w = box.width,
						h = box.height;
					ctx.strokeRect(x, y, w, h);
				}
			}
			// Render some extra text on the top left
			ctx.font = '12px Ubuntu';
			var fx = 5;
			var fy = 10;
			if (!this.runner.crashed) {
				var txt = 'obstacles: ' + (this.hor.obstacles.length) + ' pos: (' + (this.rex.xPos) + ', ' + (this.rex.yPos) + ')';
				txt += ' speed: ' + (Math.max(Math.ceil(this.runner.currentSpeed * 100) / 100, 2.8));
				ctx.fillText(txt, fx, fy);
				fy += 15;
				txt = 'velocity: ' + (this.rex.jumpVelocity);
				ctx.fillText(txt, fx, fy);
				// Render disance and threshold
				fx = (this.rex.xPos - 10);
				fy = (this.rex.yPos + 50);
				ctx.font = '10px Ubuntu';
				if (!this.rex.jumping) {
					var logDistance = this.distance ? Math.round(this.distance) : "(null)";
					txt = 'dist: ' + logDistance;
					ctx.fillText(txt, fx, fy);
					fy -= 10;
					txt = 'threshold: ' + (this.threshhold ? this.threshhold : "(null)");
					ctx.fillText(txt, fx, fy);
				} else {
					txt = '(jumping)';
					ctx.fillText(txt, fx, fy);
				}
			} else {
				ctx.fillText('i died :(', fx, fy);
			}
		}
		log(...args) {
			if (!this.opts.log) return;
			args.unshift('RunAI:');
			console.log.apply(console, args);
		}
		fireEvent(evtName) {
			if (evtName == 'draw') {
				this.draw();
			}
		}
		jump() {
			this.runner.onKeyDown({
				target: this.runner.canvas,
				keyCode: 32
			});
		}
		duck(time) {
			this.runner.onKeyDown({
				target: this.runner.canvas,
				keyCode: 40,
				preventDefault: () => {} // To prevent odd error
			});
			setTimeout(() => {
				this.runner.onKeyUp({
					target: this.runner.canvas,
					keyCode: 40,
					preventDefault: () => {} // To prevent odd error
				});
			}, time);
		}
		constructor() {
			window.rai = this; // rai = abbreviation for RunAI
			this.opts = {
				// General options
				log: true, // Extra logging
				draw: true // Draw debug info to canvas
			}
			this.runner = window.Runner.instance_;
			this.rex = this.runner.tRex;
			this.hor = this.runner.horizon;
			this.obs = this.hor.obstacles;
			this.firstStarted = false;
			this.lastPos = -9999;
			this.samePosCount = 0;
			this.restarting = false;

			// Add panel
			this.panel = document.createElement('center');
			this.dbgBtn = document.createElement('button');
			this.dbgBtn.innerHTML = 'Toggle debug drawing';
			this.dbgBtn.style.float = 'none';
			this.dbgBtn.style.fontFamily = 'Ubuntu';
			this.dbgBtn.onclick = () => window.rai.opts.draw = !window.rai.opts.draw;
			this.panel.appendChild(this.dbgBtn);
			// Line break
			this.panel.appendChild(document.createElement('br'));
			this.panel.appendChild(document.createElement('br'));
			// Little credit thing at the bottom to let people know I actually made this
			this.creditElm = document.createElement('span');
			this.creditElm.style.fontFamily = 'Ubuntu';
			this.creditElm.style.fontSize = '14px';
			this.creditElm.innerHTML = 'By Josh Auten';
			this.panel.appendChild(this.creditElm);
			document.body.appendChild(this.panel);
		}
		play() {
			this.jump(); // Starts the game
			if (!this.firstStarted) {
				this.firstStarted = true;
				var me = this;
				setInterval(() => {
					me.decide.call(me);
				}, 1);
			}
		}
		decide() {
			// This function is called at an interval that make's the rex's decisions
			var closestObj = this.hor.obstacles[0];
			if (this.runner.crashed && !this.restarting) { // the AI died
				this.restarting = true;
				this.log('i died, restarting in half a second, i died at speed', this.runner.currentSpeed, 'with highscore', rai.runner.highestScore);
				var me = this;
				setTimeout(() => {
					// Collect information about the death
					const deathInfo = {
						obs: this.hor.obstacles,
						speed: this.runner.currentSpeed,
						dist: this.runner.distanceRan,
						runningTime: this.runner.runningTime,
						time: this.runner.time
					};
					if (!localStorage["deaths"]) {
						var deaths = [deathInfo];
						localStorage["deaths"] = JSON.stringify(deaths);
						this.log('Created local death info.');
					} else {
						var deaths = JSON.parse(localStorage["deaths"]);
						deaths.push(deathInfo);
						localStorage['deaths'] = JSON.stringify(deaths);
						this.log('Updated local death info.');
					}
					me.runner.restart();
					me.restarting = false;
				}, 500);
				return;
			}
			if (this.restarting) return; // Don't make decisions if dead
			if (!closestObj) return; // No need to make a decision if there is no object near
			if (this.rex.jumping) return; // Rex isn't jumping, no decision needed
			// Update last position
			this.lastPos = this.rex.xPos;
			var ox = closestObj.xPos,
				oy = closestObj.yPos,
				x = this.rex.xPos,
				y = this.rex.yPos,
				size = closestObj.size,
				speed = this.runner.currentSpeed,
				distance = ox - x,
				threshhold = RunAI.THRESHOLD_MAP[size];
			// Adjust the distance for the speed level
			if (speed < 7.8) {
				distance += 20;
			} 
			if (speed > 8) {
				distance -= 30;
			}
			if (speed > 10) {
				distance -= 60;
			}
			if (speed > 12) { // Max speed
				distance -= 55;
				// Adjust for large single cactus'
				if (closestObj.typeConfig.type == "CACTUS_LARGE" && size == 1) {
					distance += 15;
					this.log("Large single cactus detected at high speed, adjusting distance.");
				}
			}
			// For drawing
			this.distance = distance;
			this.threshhold = threshhold;
			// Decide if it's time to jump
			if (distance <= threshhold) {
				this.log('jumping size', size, 'with distance', distance, 'at speed', speed);
				this.jump();
			}
		}
	}
	RunAI.THRESHOLD_MAP = {
		1: 185,
		2: 135,
		3: 80
	};
	RunAI.cactusMap = {
		"CACTUS_LARGE": "Large",
		"CACTUS_SMALL": "Small"
	};
	let init = () => {
		var rai = new RunAI();
		setTimeout(() => {
			rai.play();
		}, 100);
	}
	var intv = setInterval(() => {
		// Init RunAI once the runner is created
		if (window.Runner) {
			init();
			clearInterval(intv);
			return;
		}
	}, 40);
})();