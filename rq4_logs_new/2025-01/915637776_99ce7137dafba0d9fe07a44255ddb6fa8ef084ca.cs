using Sandbox;
using Sandbox.Citizen;
using Sandbox.UI;
using System.Xml.Linq;

public partial class President : Actor
{
	[Property]
	public Candidate Self { get; private set; }

	protected override void OnStart()
	{
		base.OnStart();
	}

	public override void Clothe()
	{
		// I do this manually
	}

	public override void Talk( Player target )
	{
		base.Talk( target );

		var randomMessage = ElectionsManager.CleanMessage( Game.Random.FromList( InteractPhrases ), Self, out var isAboutOpponent );
		var speechSpeed = 30f;
		var waitDuration = 2f;
		SpeechUI.AddSpeech( FullName, randomMessage, speechSpeed, waitDuration, GameObject, Gender );

		LookingTo = target;
		var talkDuration = randomMessage.Count() / speechSpeed;
		var duration = talkDuration + waitDuration;
		StopTalking = talkDuration;
		StopLooking = duration;
		Interaction.InteractionCooldown = duration;

		if ( isAboutOpponent )
			AngryFace();
		else
			HappyFace();
	}

	public override void StopLook()
	{
		base.StopLook();
		NeutralFace();
	}
}