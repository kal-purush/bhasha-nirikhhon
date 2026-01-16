
module Utils{
	
	export class Random{
		
		private adjectives:string[] = ['black','angry','happy', 'valiant', 'eager'];
		private subjects:string[] = ['cat', 'eagle', 'penguin', 'fish', 'crab', 'bear', 'bird', 'human'];
		private number: number = 10000;
		
		public username()
		{
			return this.randAdj() + '-' +this.randSubj() +'-'+this.randNumber();
		}
		
		private randAdj():string
		{
			return this.adjectives[Math.floor(Math.random()*this.adjectives.length)];
		} 
		
		private randSubj():string
		{
			return this.subjects[Math.floor(Math.random()*this.subjects.length)];
		}
		
		private randNumber():number
		{
			return Math.floor(Math.random()*this.number);
		}
		
	}
	
}
