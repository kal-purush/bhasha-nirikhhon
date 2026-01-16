
class Semaphore{

	public int value;
	
	Semaphore(){
		this.value = 0;
		//list of waiting processes
	}

	public void signal(Semaphore s){
		value++;
		if(value <= 0){
			//remove from list
		}
	}


	public void wait(Semaphore s){
		value--;
		if(value <0){
			//add to list
			//wait
		}
	}


}