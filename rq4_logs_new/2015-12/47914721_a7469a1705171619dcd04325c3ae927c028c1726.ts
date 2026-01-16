/// <reference path="Entity.ts" />
/// <reference path="Job.ts" />

enum GardenerState{WAITING, WORKING}

class Gardener extends Entity {
	
	jobs: Array<Job>;
	state: GardenerState = GardenerState.WAITING;
	speed: number = 1;
	job: Job;
	x: number = 0;
	y: number = 0;

	constructor(x, y, jobs:Array<Job>, spritesheet:Spritesheet) {
		super(spritesheet);
		this.x = x;
		this.y = y;
		this.plane = new Plane(x, y, 11, 16, 16, Color.WHITE, spritesheet.getUVFromName("worker"));
	}

	update(jobs:Array<Job>){
		if(this.state == GardenerState.WORKING){
			if(this.x < this.job.targetX){
				this.x += this.speed;
			}

			if(this.x > this.job.targetX){
				this.x -= this.speed;
			}

			if(this.y < this.job.targetY){
				this.y += this.speed;
			}

			if(this.y > this.job.targetY){
				this.y -= this.speed;
			}

			if(this.x == this.job.targetX && this.y == this.job.targetY){
				console.log("Done");
			}
		}

		this.plane.setPosition(this.x, this.y);

		if (this.state == GardenerState.WAITING){
			for (var i = 0; i < jobs.length; i++){
				if(jobs[i].status == JobStatus.WAITING){
					this.job = jobs[i];
					this.state = GardenerState.WORKING;
				}
			}
		}
	}
}