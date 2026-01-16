class BiteFish extends Fish {

  constructor(options) {
    super(options); // Call super to run the code inside `Fish`'s constructor
    this.imageUri = '/images/bitefish.gif'; // Set the image
  }

  update(t) {
    super.update(t);
    //if fish is not a bite fish or starter but has a bite fish close to it
    let surroundingFish = this.tank.getProximateDenizens(this.position,60);
    for (const fish of surroundingFish) {
      if (fish.constructor.name !== 'BiteFish' && fish.constructor.name !== 'Starter') {
        fish.kill();
      }
    }
  }
    
}