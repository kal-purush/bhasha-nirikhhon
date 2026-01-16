
const config = {
	width: 700,
	height: 400,
	type: Phaser.AUTO,
	physics: {
		default: 'arcade',
		arcade: {
			gravity: {y: 700}
		} 
	},
	scene: {
		preload: preload,
		create: create,
		update: update,
	}
}

var game = new Phaser.Game(config);

let star;
let speed = 2.5;
let platform;
let sol;

let bomb;
let prochain_lancement_sol = new Date().getTime()+250+(Math.random()*1000);
let prochain_lancement_ciel = new Date().getTime()+250+(Math.random()*1000);
let tableau_obstacles = [];
let obstacle_speed = 3;
let vitesse_angle = 10;
let obstacle_sol_debit = 10000;
let obstacle_ciel_debit = 4000;

let temps_initial = new Date().getTime();
let compteur_temps = 0;
let temps_initial_html = new Date().getTime();
let compteur_temps_html = 0;
let compteur_secondes = 0;




function preload() {

	this.load.image('star', 'assets/star.png');
	this.load.image('platform', 'assets/platform.png');
	this.load.image('bomb1', 'assets/bomb1.png');
	this.load.image('bomb2', 'assets/bomb2.png');
	this.load.image('sky', 'assets/ciel.jpeg');
	this.load.image('poisson', 'assets/poisson.png');
	this.load.image('eclair', 'assets/eclair.png');
	//this.load.image('madame', 'assets/pole.png');

}

//--------------------------------------------------------//

function create()  {


	sky = this.add.image(0, 0, 'sky');
	sky.scale = 1;

	//madame = this.add.image(650, 60, 'madame');
	//madame.scale = 0.3;





	star = this.physics.add.sprite(200, 285, 'star');
	star.body.collideWorldBounds = true;
	star.setBounce(0.5);



//--------------------------------------------------------//
						//SOL//
//--------------------------------------------------------//
	

	sol1 = this.physics.add.image(0, 400, 'platform');
	sol1.setImmovable(true);
	sol1.body.allowGravity = false;
	sol1.body.collideWorldBounds = true;
	sol1.scale = 1;

	sol2 = this.physics.add.image(500, 400, 'platform');
	sol2.setImmovable(true);
	sol2.body.allowGravity = false;
	sol2.body.collideWorldBounds = true;
	sol2.scale = 1;

	this.physics.add.collider(sol1, star);
	this.physics.add.collider(sol2, star);

//--------------------------------------------------------//
					//PLATEFORMES//
//--------------------------------------------------------//
	
	let liste_plateformes_x = [475, 200, 200, 475];
	let liste_plateformes_y = [275, 320, 225, 175];

	i = 0;
	while (i<4) {
		x_plateformes = liste_plateformes_x[i];
		y_plateformes = liste_plateformes_y[i];

		platform = this.physics.add.image(x_plateformes, y_plateformes, 'platform');
		platform.setImmovable(true);
		platform.body.allowGravity = false;
		platform.body.collideWorldBounds = true;
		platform.scale = 0.5;

		this.physics.add.collider(platform, star);

		i++;
	}

//--------------------------------------------------------//
					//TOUCHES CLAVIER//
//--------------------------------------------------------//

	Key_Z = this.input.keyboard.addKey(Phaser.Input.Keyboard.KeyCodes.Z);
	Key_Q = this.input.keyboard.addKey(Phaser.Input.Keyboard.KeyCodes.Q);
	Key_S = this.input.keyboard.addKey(Phaser.Input.Keyboard.KeyCodes.S);
	Key_D = this.input.keyboard.addKey(Phaser.Input.Keyboard.KeyCodes.D);
	Key_P = this.input.keyboard.addKey(Phaser.Input.Keyboard.KeyCodes.P);
	Key_O = this.input.keyboard.addKey(Phaser.Input.Keyboard.KeyCodes.O);

//--------------------------------------------------------//
	

}


//--------------------------------------------------------//
				//FONCTION OBSTACLES SOLS//
//--------------------------------------------------------//



function lancement_obstacle_sol(element)  {
	
	var catalogue_projectiles = ["bomb1", "bomb1", "bomb2", "bomb2"];
	var catalogue_aleatoire = Math.floor(Math.random()*4);
	
	if (new Date().getTime() > prochain_lancement_sol)  {
		
		bomb = element.physics.add.sprite(700, 300, catalogue_projectiles[catalogue_aleatoire]);
		bomb.body.colliderWorldBounds = true;
		bomb.scale = 2;
		element.physics.add.collider(sol1, bomb);
		element.physics.add.collider(sol2, bomb);
		element.physics.add.collider(star, bomb, function() {
			star.destroy();
			//bomb.destroy();
			

		})
		prochain_lancement_sol = new Date().getTime()+(Math.random()*obstacle_sol_debit);
		tableau_obstacles.push(bomb);

	}

	for (var i in tableau_obstacles)  {
		bomb = tableau_obstacles[i];
		bomb.x -= obstacle_speed;
		bomb.angle -= vitesse_angle;
		if(bomb.x < 0)  {
			tableau_obstacles.shift;
			bomb.destroy();
	
		}
	}
}
//--------------------------------------------------------//
				//FONCTION OBSTACLES CIEL//
//--------------------------------------------------------//

	function lancement_obstacles_ciel(element, x)  {

		if (new Date().getTime() > prochain_lancement_ciel)  {

			poisson = element.physics.add.sprite(x, 0, 'poisson');
			poisson.scale = 0.026;
			poisson.angle = 270;

			prochain_lancement_ciel = new Date().getTime()+250+(Math.random()*obstacle_ciel_debit);
			element.physics.add.collider(star, poisson, function() {
				star.destroy();
			})
		}

	}

//--------------------------------------------------------//


function update()  {


//--------------------------------------------------------//
					//CHRONOMÈTRE//
//--------------------------------------------------------//
	

	compteur_temps_html = new Date().getTime() - temps_initial_html;
	compteur_temps1 = new Date().getTime() - temps_initial;
	
	document.getElementById('compteur').innerHTML = compteur_secondes;

	if (compteur_temps_html > 1000)  {
		temps_initial_html = new Date().getTime();
		compteur_temps_html = 0;
		compteur_secondes ++;
	}

	
	
//--------------------------------------------------------//
					//DÉPLACEMENTS//
//--------------------------------------------------------//


	if(Key_Z.isDown && star.body.touching.down)  {
		star.setVelocity(0, -300);
	}
	if(Key_Q.isDown)  {
		star.x -= speed;
		star.angle -= 10;
	}
	if(Key_D.isDown)  {
		star.x += speed;
		star.angle +=10;
	}
	if(Key_P.isDown)  {
		star.setVisible(false);
	}
	if(Key_O.isDown)  {
		star.setVisible(true);
	}

//--------------------------------------------------------//
					//LANCEMENT OBSTACLES//
//--------------------------------------------------------//
	
	lancement_obstacle_sol(this);
	lancement_obstacles_ciel(this, Math.random()*700);


//--------------------------------------------------------//

	if (compteur_temps1 > 5000)  {
		temps_initial = new Date().getTime();
		compteur_temps = 0;
		speed += 0.5;
		obstacle_sol_debit *= 0.7;
		obstacle_ciel_debit *= 0.7;
		obstacle_speed += 0.1;
		vitesse_angle +=1;
	}



}


