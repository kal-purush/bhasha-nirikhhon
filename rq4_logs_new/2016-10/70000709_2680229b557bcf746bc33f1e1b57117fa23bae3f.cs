using UnityEngine;
using System.Collections;
/// <summary>
/// Dynamic particle.
/// 
/// The dynamic particle is the backbone of the liquids effect. Its a circle with physics with 3 states, each state change its physic properties and its sprite color ( so the shader can separate wich particle is it to draw)
/// The particles scale down and die, and have a scale  effect towards their velocity.
/// 
/// Visit: www.codeartist.mx for more stuff. Thanks for checking out this example.
/// Credit: Rodrigo Fernandez Diaz
/// Contact: q_layer@hotmail.com
/// </summary>

public class DynamicParticle : MonoBehaviour {	
	public enum STATES {BLUE,RED,GREEN,PURPLE,YELLOW,ORANGE,WATER,BLACK,NONE}; //The states of the particle
	public STATES currentState = STATES.NONE; //Defines the currentstate of the particle, default is water
	public GameObject currentImage; //The image is for the metaball shader for the effect, it is onle seen by the liquids camera.
	public GameObject[] particleImages; //We need multiple particle images to reduce drawcalls
	float particleLifeTime = 3.0f, startTime;//How much time before the particle scalesdown and dies	
	public float particleSize = 4.0f;
	public float originalParticleSize = 4.0f;
	
	bool haveParticlesHit;
	public bool fixedSize = false;

	public float scaleValue;

	public LevelManager levelmanager;
	public Checkpoint checkpoint;

	public void Awake(){ 
		if (currentState == STATES.NONE) {
			SetState (STATES.BLUE);
		}
		haveParticlesHit = false;
		levelmanager = FindObjectOfType<LevelManager>();
		checkpoint = FindObjectOfType<Checkpoint>();
	}

	public void Start() {
		originalParticleSize = particleSize;
	}

	//The definitios to each state
	public void SetState(STATES newState){
		if(newState!=currentState){ //Only change to a different state
			switch(newState){
				case STATES.BLUE:													
					GetComponent<Rigidbody2D>().gravityScale=2f;
					GetComponent<Rigidbody2D>().mass = 1f;
					GetComponent<Rigidbody2D>().drag = 1.5f;
				break;
				case STATES.YELLOW:		
					//gameObject.layer=LayerMask.NameToLayer("Gas");// To have a different collision layer than the other particles (so gas doesnt rises up the lava but still collides with the wolrd)
				break;		
				case STATES.ORANGE:		
				break;	
				case STATES.RED:
				break;
				case STATES.PURPLE:
				break;
				case STATES.GREEN:		
				break;	
				case STATES.WATER:		
					GetComponent<Rigidbody2D>().gravityScale=0.8f;
					GetComponent<Rigidbody2D>().mass = 0.2f;
					GetComponent<Rigidbody2D>().drag = 0f;
				break;	
				case STATES.BLACK:		
					GetComponent<Rigidbody2D>().gravityScale=1.6f;
					GetComponent<Rigidbody2D>().drag = 1.5f;
				break;	
				case STATES.NONE:
					//Destroy(gameObject);
				break;
			}
			if(newState!=STATES.NONE){
				currentState=newState;
				startTime = Time.time;//Reset the life of the particle on a state change
				GetComponent<Rigidbody2D>().velocity=new Vector2();	// Reset the particle velocity	
				currentImage.SetActive(false);
				currentImage=particleImages[(int)currentState];
				currentImage.SetActive(true);
			}
		}		
	}
	void Update () {
		MovementAnimation();

		ScaleDown ();
	}
	// This scales the particle image acording to its velocity, so it looks like its deformable... but its not ;)
	void MovementAnimation(){
		Vector3 movementScale=new Vector3(1.0f,1.0f,1.0f);//Tamaño de textura no de metaball			
		movementScale.x+=Mathf.Abs(GetComponent<Rigidbody2D>().velocity.x)/30.0f;
		movementScale.z+=Mathf.Abs(GetComponent<Rigidbody2D>().velocity.y)/30.0f;
		movementScale.y=1.0f;		
		currentImage.gameObject.transform.localScale = movementScale;
	}
	// The effect for the particle to seem to fade away
	void ScaleDown(){
		scaleValue = particleSize - ((Time.time - startTime) / particleLifeTime);
		Vector2 particleScale = Vector2.one;
		if (scaleValue < 0.5f) {
			Destroy();
		} else {
			particleScale.x = scaleValue;
			particleScale.y = scaleValue;
			transform.localScale = particleScale;
		}
	}

	void HaveParticlesHit()
	{
		haveParticlesHit = true;
	}

	protected void Destroy() {
		gameObject.SetActive(false);
		scaleValue = particleSize;
	}

	public void OnObjectReuse () {
		gameObject.SetActive(true);
		startTime = Time.time;
		gameObject.transform.localScale = new Vector3(particleSize, particleSize, particleSize);
	}

	// Sets the size of the particle
	public void SetSize(float sizex, float sizey){
		transform.localScale = new Vector2 (sizex, sizey);
		particleSize = sizex;
	}

	// Sets random size
	public void SetRandomSize(float min, float max) {
		particleSize += Random.Range (min, max);
	}

	// To change particles lifetime externally (like the particle generator)
	public void SetLifeTime(float time){
		particleLifeTime = time;	
	}

	// Here we handle the collision events with another particles, in this example water+lava= water-> gas
	void OnCollisionEnter2D(Collision2D other){

		//BLUE PAINT
		if(currentState==STATES.BLUE && other.gameObject.tag=="white")
		{
			other.gameObject.tag = "blue";
		}
		//Blue paint collision when player = red
		if(currentState==STATES.BLUE && other.gameObject.tag=="red")
		{
			other.gameObject.tag = "purple";
		}
		//Blue paint collision when player = yellow
		if(currentState==STATES.BLUE && other.gameObject.tag=="yellow")
		{
			other.gameObject.tag = "green";
		}

		//RED PAINT
		if(currentState==STATES.RED && other.gameObject.tag=="white")
		{
			other.gameObject.tag = "red";
		}
		//Red paint collision when player = blue
		if(currentState==STATES.RED && other.gameObject.tag=="blue")
		{
			other.gameObject.tag = "purple";
		}
		//Red paint collision when player = yellow
		if(currentState==STATES.RED && other.gameObject.tag=="yellow")
		{
			other.gameObject.tag = "orange";
		}

		//YELLOW PAINT
		if(currentState==STATES.YELLOW && other.gameObject.tag=="white")
		{
			other.gameObject.tag = "yellow";
		}
		//Yellow paint collision when player = blue
		if(currentState==STATES.YELLOW && other.gameObject.tag=="blue")
		{
			other.gameObject.tag = "green";
		}
		//Yellow paint collision when player = red
		if(currentState==STATES.YELLOW && other.gameObject.tag=="red")
		{
			other.gameObject.tag = "orange";
		}

		//ORANGE PAINT
		if(currentState==STATES.ORANGE && other.gameObject.tag=="white")
		{
			other.gameObject.tag = "orange";
		}
		//GREEN PAINT
		if(currentState==STATES.GREEN && other.gameObject.tag=="white")
		{
			other.gameObject.tag = "green";
		}
		//PURPLE PAINT
		if(currentState==STATES.PURPLE && other.gameObject.tag=="white")
		{
			other.gameObject.tag = "purple";
		}
		//WATER
		if(currentState==STATES.WATER && other.gameObject.tag=="blue" || currentState==STATES.WATER && other.gameObject.tag=="red" || currentState==STATES.WATER && other.gameObject.tag=="yellow" || currentState==STATES.WATER && other.gameObject.tag=="purple" || currentState==STATES.WATER && other.gameObject.tag=="green" || currentState==STATES.WATER && other.gameObject.tag=="orange")
		{
			other.gameObject.tag = "white";
		}
		//BLACK PAINT
		if(currentState==STATES.BLACK && other.gameObject.tag=="blue" || currentState==STATES.BLACK && other.gameObject.tag=="red" || currentState==STATES.BLACK && other.gameObject.tag=="yellow" || currentState==STATES.BLACK && other.gameObject.tag=="purple" || currentState==STATES.BLACK && other.gameObject.tag=="green" || currentState==STATES.BLACK && other.gameObject.tag=="orange" || currentState==STATES.BLACK && other.gameObject.tag =="white")
		{
			if (other.gameObject.tag == "white")
			{
				other.gameObject.tag = "white";
			}
			if (other.gameObject.tag == "red")
			{
				other.gameObject.tag = "red";
			}
			if (other.gameObject.tag == "blue")
			{
				other.gameObject.tag = "blue";
			}
			if (other.gameObject.tag == "yellow")
			{
				other.gameObject.tag = "yellow";
			}
			if (other.gameObject.tag == "green")
			{
				other.gameObject.tag = "green";
			}
			if (other.gameObject.tag == "purple")
			{
				other.gameObject.tag = "purple";
			}
			if (other.gameObject.tag == "orange")
			{
				other.gameObject.tag = "orange";
			}
			levelmanager.Respawn();
		//	other.gameObject.tag = checkpoint.tempTag;
		}
		//Killing black particles when hitting stationary black object
		if(currentState==STATES.BLACK && other.gameObject.tag=="blackBox")
		{
			Destroy();
		}

		//Mixing blue and red
		if(currentState==STATES.BLUE && other.gameObject.tag=="DynamicParticle"){ 
			if(other.collider.GetComponent<DynamicParticle>().currentState==STATES.RED){
				SetState(STATES.PURPLE);
			}
		}
		if(currentState==STATES.PURPLE && other.gameObject.tag=="DynamicParticle"){ 
			Invoke("HaveParticlesHit", .3f);
			if(other.collider.GetComponent<DynamicParticle>().currentState==STATES.BLUE && haveParticlesHit == true || other.collider.GetComponent<DynamicParticle>().currentState==STATES.RED && haveParticlesHit == true)
			{
				other.collider.GetComponent<DynamicParticle>().SetState(STATES.PURPLE);
			}
		}
		//Mixing blue and yellow
		if(currentState==STATES.BLUE && other.gameObject.tag=="DynamicParticle"){ 
			if(other.collider.GetComponent<DynamicParticle>().currentState==STATES.YELLOW){
				SetState(STATES.GREEN);
			}
		}
		if(currentState==STATES.GREEN && other.gameObject.tag=="DynamicParticle"){ 
			Invoke("HaveParticlesHit", .3f);
			if(other.collider.GetComponent<DynamicParticle>().currentState==STATES.BLUE && haveParticlesHit == true|| other.collider.GetComponent<DynamicParticle>().currentState==STATES.YELLOW && haveParticlesHit == true)
			{
				other.collider.GetComponent<DynamicParticle>().SetState(STATES.GREEN);
			}
		}
		//Mixing Yellow and red
		if(currentState==STATES.YELLOW && other.gameObject.tag=="DynamicParticle"){ 
			if(other.collider.GetComponent<DynamicParticle>().currentState==STATES.RED){
				SetState(STATES.ORANGE);
			}
		}
		if(currentState==STATES.ORANGE && other.gameObject.tag=="DynamicParticle"){ 
			Invoke("HaveParticlesHit", .3f);
			if(other.collider.GetComponent<DynamicParticle>().currentState==STATES.YELLOW && haveParticlesHit == true || other.collider.GetComponent<DynamicParticle>().currentState==STATES.RED && haveParticlesHit == true)
			{
				other.collider.GetComponent<DynamicParticle>().SetState(STATES.ORANGE);
			}
		}
		//When Black hits anything
		if(currentState==STATES.BLACK && other.gameObject.tag=="DynamicParticle"){ 
			Invoke("HaveParticlesHit", .3f);
			if(other.collider.GetComponent<DynamicParticle>().currentState==STATES.BLUE && haveParticlesHit == true || other.collider.GetComponent<DynamicParticle>().currentState==STATES.RED && haveParticlesHit == true || other.collider.GetComponent<DynamicParticle>().currentState==STATES.YELLOW && haveParticlesHit == true || other.collider.GetComponent<DynamicParticle>().currentState==STATES.ORANGE && haveParticlesHit == true || other.collider.GetComponent<DynamicParticle>().currentState==STATES.GREEN && haveParticlesHit == true || other.collider.GetComponent<DynamicParticle>().currentState==STATES.PURPLE && haveParticlesHit == true)
			{
				other.collider.GetComponent<DynamicParticle>().SetState(STATES.BLACK);
			}
		}
		//When Water hits Black
		if(currentState==STATES.WATER && other.gameObject.tag=="DynamicParticle"){ 
			if(other.collider.GetComponent<DynamicParticle>().currentState==STATES.BLACK)
			{
				//gameObject.SetActive(false);
				Destroy();
				//Destroy(other.gameObject);
			}
		}
	}
}