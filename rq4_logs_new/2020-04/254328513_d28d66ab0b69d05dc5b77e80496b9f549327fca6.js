let countries = {
	USA: {
    nation:"USA",
		x: 228,
		y: 245,
		radius: 60,
		infos: "asdfasdfasdasf"
	},
	Germany: {
		nation:"Deutschland",
		x: 605,
		y: 201,
		radius: 15,
		infos: " "
	},
	France: {
		nation:"Frankreich",
		x: 580,
		y: 220,
		radius: 17,
		infos:"bonjour",
	},
	Brazil: {
		nation:"Brasilien",
		x: 403,
		y: 420,
		radius: 50,
		infos:"",
	},
	Canada:{
		nation:"Kanada",
		x:188,
		y:168,
		radius:70,
		infos:"hello",
	},
	Australia:{
		nation:"Australien",
		x:1025.3333740234375 ,
		y:460.33333587646484,
		radius:50,
		infos:"hello",
	},
	Russia:{
		nation:"Russland",
		x:883,
		y:148,
		radius:90,
		infos:"hah",
	},
	Namibia:{
		nation:"Namibia",
		x:629.6666870117188,
		y:454,
		radius:15,
		infos:"",

	},
	Argentinien:{
		nation:"Argentinien",
		x:350.3333435058594,
		y:500.6666717529297,
		radius:30,
		infos:"",
	},

	}








class Country {
	constructor(countryName, xCord,yCord, radius, infos) {
        this.nation = countryName;
        this.x = xCord;
		this.y = yCord;
        this.radius = radius;
		this.infos = infos;
    }
  display(){
    fill ("red");
circle(this.x, this.y,this.radius);
  }
 clicked(){
	  let d=dist(mouseX, mouseY, this.x, this.y);
	  if(d<=this.radius){
		  game.clickedCountry = this.nation;
  } 
}

}