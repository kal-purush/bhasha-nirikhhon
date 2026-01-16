/// <reference path="Job.ts" />
/// <reference path="JobDelegate.ts" />

class PlantJob extends Job {

	delegate: JobDelegate;
	wait: number = 0;
	
	constructor(targetX, targetY, delegate:JobDelegate){
		super(targetX, targetY);
		this.delegate = delegate;
	}

	update(){
		if(this.gardener.x > this.targetX){
			this.gardener.move(-1, 0);
		}

		if(this.gardener.x < this.targetX){
			this.gardener.move(1, 0);
		}

		if(this.gardener.y <  this.targetY){
			this.gardener.move(0, 1);
		}

		if(this.gardener.y > this.targetY){
			this.gardener.move(0, -1);
		}

		if(this.gardener.x == this.targetX && this.gardener.y == this.targetY){
			if(this.wait >= 120){
				this.delegate.finishedJob(this.targetX, this.targetY, JobType.PLANT);
				this.finished();
			}
			this.wait++;
		}
	}
}