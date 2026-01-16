
deleteVariables("Ability::*");

// Abilities
$SkillRestriction["magebolt"] = $MinLevel @" 15 G Wizard";
$SkillRestriction["bladebolt"] = $MinLevel @" 15 C Fighter";

$Ability::keyword[0] = "magebolt";
$Ability::index[magebolt] = 0;
$Ability::baseCooldown[0] = 15;
$Ability::SoundId[0] = UnravelAM;
$Ability::cost[0,Mana] = 20;
$Ability::cost[0,Stam] = 5;
$Ability::cost[0,Item] = "";

//$AbilityCost["#bladebolt"] = "Mana 15";


function Player::useAbility(%clientId,%ability)
{
    %idx = $Ability::index[%ability];
    if(%idx != "")
    {
        if(SkillCanUse(%clientId, %ability))
        {
            if(Ability::CheckCost(%clientId,%idx))
            {
                if(%idx == 0)
                {
                    Ability::DoMageBolt(%clientId);
                }
                else if(%idx == 1)
                {
                
                }
            }
            else
                Client::sendMessage(%clientId, $MsgWhite, $AbilityFailReason);
        }
        else
            Client::sendMessage(%clientId, $MsgRed, "You lack the requirements to use that ability.");
    }
    else
        Client::sendMessage(%clientId, $MsgWhite, "Ability "@%cropped@" does not exist.");
}

function Ability::DoAbilityCost(%clientId,%idx)
{
    if($Ability::cost[%idx,Mana])
        refreshMANA(%clientId,$Ability::cost[%idx,Mana]);

    if($Ability::cost[%idx,Stam])
        refreshStamina(%clientId,$Ability::cost[%idx,Stam]);
        
    if($Ability::cost[%idx,Item])
        TakeThisStuff(%clientId,$Ability::cost[%idx,Item]);
}

function Ability::CheckCost(%clientId,%idx)
{
    $AbilityFailReason = "";
    %flag = true;
    if($Ability::cost[%idx,Mana])
    {
        %mana = fetchData(%clientId,"MANA");
        if(%mana < $Ability::cost[%idx,Mana])
            %flag = false;
    }
    
    if(!%flag)
    {
        $AbilityFailReason = "You do not have enough mana ("@$Ability::cost[%idx,Mana]@")";
        return false;
    }
    
    if($Ability::cost[%idx,Stam])
    {
        %stam = fetchData(%clientId,"Stamina");
        if(%stam < $Ability::cost[%idx,Stam])
            %flag = false;
    }
    
    if(!%flag)
    {
        $AbilityFailReason = "You do not have enough stamina ("@$Ability::cost[%idx,Stam]@")";
        return false;
    }
    
    if($Ability::cost[%idx,Item])
    {
        if(!HasThisStuff(%clientId,$Ability::cost[%idx,Item]))
            %flag = false;
    }
    
    if(!%flag)
    {
        $AbilityFailReason = "You lack the necessary items ("@$Ability::cost[%idx,Item]@")";
        return false;
    }
    
    return true;
}

function Ability::DoMageBolt(%clientId)
{
    %timeLeft = fetchData(%clientId,"blockMageBolt");
    if(%timeLeft == "")
    {
        %trans = Gamebase::getEyeTransform(%clientId);
        %player = Client::getOwnedObject(%clientId);
        %vel = Item::getVelocity(%player);
        Ability::DoAbilityCost(%clientId,$Ability::index[magebolt]);
        
        playSound($Ability::SoundId[$Ability::index[magebolt]],Gamebase::getPosition(%clientId));
        
        //Projectile::spawnProjectile(MageBoltMain,%trans,%player,%vel);
        Projectile::spawnProjectile(MageBoltTail,%trans,%player,%vel);
        
        storeData(%clientId,"blockMageBolt",getSimTime() + 3);
        schedule("storeData("@%clientId@",\"blockMageBolt\",\"\");",3);
    }
    else
    {
        %time = Number::Beautify(%timeLeft - getSimTime(),0,2);
        Client::sendMessage(%clientId, $MsgWhite, "That ability is still on cooldown. ("@%time@"s)");
    }
}