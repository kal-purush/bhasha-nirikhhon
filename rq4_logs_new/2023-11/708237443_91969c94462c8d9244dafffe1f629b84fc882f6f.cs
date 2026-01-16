
$AITestName = "TestGob";
$AITestArmor = GoblinArmor;

function SpawnTestAIAtLOS(%clientId,%team,%name,%armor)
{
    %player = Client::getOwnedObject(%clientId);
    
    $los::position = "";
    %los = Gamebase::getLOSInfo(%player,500);
    
    if(%los)
    {
        
        %rot = vector::add(Gamebase::getRotation(%clientId),"0 0 "@ $pi);
    
        %aiName = SpawnTestAI(%name,%armor,%team,$los::position,%rot);
        
        %aiId = AI::getId(%aiName);
        
        GameBase::startFadeIn(%aiId);
		PlaySound(SoundSpawn2, $los::position);
        
        return %aiName;
    }
    
    return -1;
}

function GiveAISmarts(%aiName,%per)
{
    AI::SetVar(%aiName, triggerPct, 1.0 );
	AI::setVar(%aiName, iq, 100 );
	AI::setVar(%aiName, attackMode, $AIattackMode);
    AI::setVar(%aiName, pathType, $AI::defaultPathType);
    AI::SetVar(%aiName, spotDist, $AIspotDist);
    if(%per != "")
        ai::callbackPeriodic(%aiName, 5, AI::DumbPeriodic);
}

function AI::DumbPeriodic(%aiName)
{

}

function aCallBackTest(%aiName)
{
    echo("Callback 1");
}

function aCallBackTest2(%aiName)
{
    echo("Callback 2");
}

function aCallBackTest3(%aiName,%b,%c)
{
    echo("Callback 3");
    echo(%aiName);
    echo(%b);
    echo(%c);
}

function SpawnTestAI(%name,%armor,%team,%pos,%rot)
{
    %n = getAInumber();
    %aiName = %name@%n;
    AI::otherCreate(%aiName,%aiName,%armor,%pos,%rot);
    
    setAINumber(%aiName,%n);
    %aiId = AI::getId(%aiName);
    GameBase::setTeam(%aiId, %team);
    
    return %aiName;
}

// AI::SetVar(%name,PathType,..) findings
// Path Type 0 - Circular
// Bot will follow AI::DirectiveWaypoints starting from lowest order number to highest.  Once at highest, it will move back to lowest and repeat
// Path Type 1 - One Way
// Bot will follow AI::DirectiveWaypoints starting from lowest order number to highest.  Once at highest, the bot will stop, unless a new highest is given
// Path Type 2 - Two Way
// Bot will follow AI::DirectiveWaypoints starting from lowest order number to highest.  Once at highest, it will reverse the order and move back to lowest.
// Interesting behavior for 2.  If already stopped at a position becauese of path type 1, if you switch to path type 2, the both will not move unless you give it a new (or refresh and old) waypoint.

// Setting to scripted targets, ai will ignore all targets not set via directive target.
// Targets in the directive target list have no priority by order number.  AI just targets the closest that is on that list.

// AI need a callback periodic, or they won't works.

//Only AI::DirectiveCallback1 seems to work.  Was unable to get it to work for follow directives.
// For waypoint directives, the callback is called.  Just %ainame for parameter.  For Target directives, the %ainame and %client are parameters

// AI::setVar for seekOff is the offset from the player the AI tries to maintain when following.
// "3 3 0" Would be to the NE.  So "N E Up"
// Matches Vector::getFromRot(Gamebase::getRotation(target))

//function AI::onTargetLOSAcquired(%aiName, %idNum)
// Target is in range and visible

//function AI::onTargetLOSLost(%aiName, %idNum)
// Target had been acquired, but not visible from LOS

//function AI::onTargetLOSRegained(%aiName, %idNum)
// Target had been acquired, went out of LOS, and then became visible again.  Never left spotDist

//function AI::onTargetDied(%aiName, %idNum)
// Self explainitory

//AI::DeadCallback
// Callback on death