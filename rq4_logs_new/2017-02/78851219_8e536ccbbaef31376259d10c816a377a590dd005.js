//expect().to....();
//matchers: toContain(), toEqual()
//use beforeEach to set up variables if they are to be used in every spec
//afterEach is the same but after each spec
//can add custom matchers using this.addMatcher({ name: function(){ return x == y}})

describe('Enemy', function() {
	var enemy1 = new Enemy(null, 10, 10, 100, 1, 20, null, 1);
	var enemy2 = new Enemy(null, 10, 10, 125, 1, 20, null, 2);
	var enemy3 = new Enemy(null, 10, 10, 150, 2, 20, null, 3);

	it('should be have null game', function() {
	    expect(enemy1.game).toEqual(null);
	});
	it('should have x of 10', function() {
	    expect(enemy1.x).toEqual(10);
	});
	it('should have y of 10', function() {
	    expect(enemy1.y).toEqual(10);
	});
	it('should have 100 health', function() {
	    expect(enemy1.hp).toEqual(100);
	});
	it('should have 1 AP', function() {
	    expect(enemy1.ap).toEqual(1);
	});
	it('should have 20 dmg', function() {
	    expect(enemy1.dmg).toEqual(20);
	});
	it('should have type of 1', function() {
	    expect(enemy1.type).toEqual(1);
	});
	it('should have type of 2', function() {
	    expect(enemy2.type).toEqual(2);
	});
	it('should have type of 3', function() {
	    expect(enemy3.type).toEqual(3);
	});
	it('should have 10 credits', function() {
	    expect(enemy1.credits).toEqual(10);
	});
	it('should have 40 credits', function() {
	    expect(enemy2.credits).toEqual(40);
	});
	it('should have 90 credits', function() {
	    expect(enemy3.credits).toEqual(90);
	});
	it('should have 5 dodge', function() {
	    expect(enemy1.dodgeChance).toEqual(5);
	});
	it('should have 10 dodge', function() {
	    expect(enemy2.dodgeChance).toEqual(10);
	});
	it('should have 15 dodge', function() {
	    expect(enemy3.dodgeChance).toEqual(15);
	});

	//check credits for three types
});