
deleteVariables("SkillBook::*");

$SkillBook::TypeGroup = 0;
$SkillBook::TypeSkill = 1;

$SkillBook::SkillCount = 0;

function SkillBook::AddSkillMenuGroup(%label,%name,%parentLabel)
{
    $SkillBook::Type[%label] = $SkillBook::TypeGroup;
    $SkillBook::SkillGroup[%label,Name] = %name;
    $SkillBook::SkillGroup[%label,Parent] = %parentLabel;
    $SkillBook::SkillGroup[%label,NumEntries] = 0;
    $SkillBook::SkillGroup[%label,Entry,0] = "";
    if(%parentLabel != "")
    {
        %cnt = $SkillBook::SkillGroup[%parentLabel,NumEntries];
        $SkillBook::SkillGroup[%parentLabel,Entry,%cnt] = %label;
        $SkillBook::SkillGroup[%parentLabel,NumEntries] += 1;
    }   
}

function SkillBook::AddSkillToGroup(%label,%groupLabel)
{    
    %cnt = $SkillBook::SkillGroup[%groupLabel,NumEntries];
    $SkillBook::SkillGroup[%groupLabel,Entry,%cnt] = %label;
    $SkillBook::SkillGroup[%groupLabel,NumEntries] += 1;
}

function SkillBook::AddSkillToList(%label,%name,%idLabel,%cmd)
{
    $SkillBook::Type[%label] = $SkillBook::TypeSkill;
    $SkillBook::SkillList[$SkillBook::SkillCount] = %label;
    $SkillBook::Skill[%label,Name] = %name;
    $SkillBook::Skill[%label,Index] = $SkillBook::SkillCount;
    $SkillBook::Skill[%label,IDLabel] = %idLabel;
    $SKillBook::Skill[%label,Command] = %cmd;
    $SkillBook::SkillCount++;
}


function SkillBook::FillSkillGroup(%groupLabel,%reqType,%opt)
{
    for(%i = 0; %i < $SkillBook::SkillCount; %i++)
    {
        %label = $SkillBook::SkillList[%i];
        %idLabel = $SkillBook::Skill[%label,IDLabel];
        %data = $SkillRestriction[%idLabel];
        
        %flag = false;
        for(%k = 0; %k < GetWordCount(%data); %k +=2)
        {
            if(getWord(%data,%k) == %reqType)
            {
                if(%opt != "")
                {
                    if(getWord(%data,%k+1) == %opt)
                        %flag = true;
                }
                else
                {
                    %flag = true;
                }
            }
            
            if(%flag)
                break;
        }
        
        %idx = %k;
        if(%flag)
        {
            %cnt = $SkillBook::SkillGroup[%groupLabel,NumEntries];
            $SkillBook::SkillGroup[%groupLabel,Entry,%cnt] = %label;
            $SkillBook::SkillGroup[%groupLabel,NumEntries] += 1;
        }
    }
}



SkillBook::AddSkillToList("talksay","Say","#say","#s message");
SkillBook::AddSkillToList("talkshout","Shout","#say","#shout message");
SkillBook::AddSkillToList("talkwhisper","Whisper","#whisper","#whisper message");
SkillBook::AddSkillToList("talksglobal","Global","#global","#g message");
SkillBook::AddSkillToList("talktell","Tell","#tell","#tell <name> message");
SkillBook::AddSkillToList("talkr","Respond","#r","#r message");
SkillBook::AddSkillToList("talkparty","Party","#party","#p message");
SkillBook::AddSkillToList("talkgroup","Group","#group","#group message");
SkillBook::AddSkillToList("talkzone","Zone","#zone","#zone message");
SkillBook::AddSkillToList("defaulttalk","Set Default Talk","#defaulttalk","#defaulttalk #g");

$AccessoryVar["#say", $MiscInfo] = "Talk to others nearby.  Also used to talk to bots";
$AccessoryVar["#whisper", $MiscInfo] = "Talk to others very nearby.";
$AccessoryVar["#shout", $MiscInfo] = "Talk to others at a distance.";
$AccessoryVar["#global", $MiscInfo] = "Talk to everyone on the server.";
$AccessoryVar["#tell", $MiscInfo] = "Privately message someone.";
$AccessoryVar["#r", $MiscInfo] = "Respond to the last person to #tell you.";
$AccessoryVar["#party", $MiscInfo] = "Talk to other players in your party.";
$AccessoryVar["#group", $MiscInfo] = "Talk to other players in your group list.";
$AccessoryVar["#zone", $MiscInfo] = "Speak to everyone in the same zone.";
$AccessoryVar["#defaulttalk", $MiscInfo] = "Sets what talk type your normal CHAT uses, without needing to prefix a talk command.";

SkillBook::AddSkillToList("setcmd","Set Keybind","#set","#set <Key> <ChatCommand>");
SkillBook::AddSkillToList("savechar","Save","#savecharacter","#savecharacter");
SkillBook::AddSkillToList("recall","Recall","#recall","#recall");
SkillBook::AddSkillToList("sleep","Sleep","#sleep","#sleep");
SkillBook::AddSkillToList("heal","Heal","#heal","#heal");
SkillBook::AddSkillToList("rest","Rest","#rest","#rest");
SkillBook::AddSkillToList("wake","Wake","#wake","#wake");

$AccessoryVar["#set", $MiscInfo] = "Binds command to the specified key.  For example \"#set b #use BluePotion\" will use a Blue Potion when you press b. And \"#set g #cast firebomb\" will cast the firebomb spell when you press g.";
$AccessoryVar["#savecharacter", $MiscInfo] = "Request the server to save your character.  The server automatically saves everyone on a schedule, but this allows you to save out of schedule.";
$AccessoryVar["#recall", $MiscInfo] = "Returns you to a major city, but you have to stand still for "@$recallDelay@"s. If you move, you have to wait the full delay before you can attempt another recall, unless if you are up or down REALLY fast.  Use if you get stuck.";
$AccessoryVar["#sleep", $MiscInfo] = "Sleep to recover both health and stamina. Can only be done in some places or at your camp. Or in the Unknown if you are a Ranger.  Being hit while sleeping will wake you up, but also do 10x damage. Use #wake to stop.";
$AccessoryVar["#heal", $MiscInfo] = "Uses up Stamina to recover your wounds. Not the quickest form of healing, but better than nothing. Being hit will wake you up, but also do 10x damage. Use #wake to stop.";
$AccessoryVar["#rest", $MiscInfo] = "Rest to recover your stamina. Being hit will wake you up, but also do 10x damage. Use #wake to stop.";
$AccessoryVar["#wake", $MiscInfo] = "Wake up from sleep, heal, and rest.";

SkillBook::AddSkillToList("getinfo","Get Info","#getinfo","#getinfo <name>");
SkillBook::AddSkillToList("setinfo","Set Info","#setinfo","#setinfo text");
SkillBook::AddSkillToList("whatis","What is?","#w","#w <ItemNameNoSpaces>");
SkillBook::AddSkillToList("craftinfo","Craft Reqs","#c","#c <ItemNameNoSpaces>");

$AccessoryVar["#getinfo", $MiscInfo] = "See another player's #setinfo text";
$AccessoryVar["#setinfo", $MiscInfo] = "Set your info text.";
$AccessoryVar["#w",$MiscInfo] = "Get information on an item, skill, or spell. The name to use doesn't always match the description, but rule of thumb is the name with no spaces or special characters. Example: Cheetaur's Paws would be \"#w cheetaurspaws\".";
$AccessoryVar["#c",$MiscInfo] = "Get crafting information on a craftable item.  Use #c <itemname> to see if it is craftable.";

SkillBook::AddSkillToList("steal","Steal","#steal","#steal");
SkillBook::AddSkillToList("pickpocket","Pickpocket","#pickpocket","#pickpocket");
SkillBook::AddSkillToList("mug","Mug","#mug","#mug");
SkillBook::AddSkillToList("mugbelt","Mug Belt","#mugbelt","#mugbelt");

$AccessoryVar["#steal", $MiscInfo] = "Steal coins from a target (player or mob). Has a chance to fail and alert the victim. The victim can attack you for a time afterwards.";
$AccessoryVar["#pickpocket", $MiscInfo] = "Steal inventory items from a target (player or mob). Has a chance to fail and alert the victim. The victim can attack you for a time afterwards.";
$AccessoryVar["#mug", $MiscInfo] = "Steal inventory and equipped items from a target (player or mob). Has a chance to fail and alert the victim. The victim can attack you for a time afterwards.";
$AccessoryVar["#mugbelt", $MiscInfo] = "***WIP*** Steal belt items from a target (player or mob). Has a chance to fail and alert the victim. The victim can attack you for a time afterwards.";

SkillBook::AddSkillToList("compass","Compass","#compass","#compass <town/dungeon>");
SkillBook::AddSkillToList("track","Track","#track","#track <name>");
SkillBook::AddSkillToList("trackpack","Track Pack","#trackpack","#trackpack <name>");
SkillBook::AddSkillToList("advcompass","Advanced Compass","#advcompass","#advcompass <town/dungeon>");
SkillBook::AddSkillToList("zonelist","Zone List","#zonelist","#zonelist");

$AccessoryVar["#compass", $MiscInfo] = "Find the cardinal direction of the nearest town or dungeon from you. Usage: #compass town/dungeon";
$AccessoryVar["#avcompass", $MiscInfo] = "Find the cardinal direction of a specific town or dungeon or any zone from you. You only need to provide part of the name.  Example: #compass Ethren";
$AccessoryVar["#track", $MiscInfo] = "Find the cardinal direction and distance of a player's or mob's last known location from you. The tracking goes off of a player's last left \"scent.\" Some skills can prevent or lessen the chance of scent.";
$AccessoryVar["#trackpack", $MiscInfo] = "Find the cardinal direction and distance of a player's pack nearest to you. Can be your or another player's. Usage: #trackpack name";
$AccessoryVar["#zonelist", $MiscInfo] = "List all the players and mobs in your current zone.";

SkillBook::AddSkillToList("hide","Hide","#hide","#hide");
SkillBook::AddSkillToList("bash","Bash","#bash","#bash");
SkillBook::AddSkillToList("flurry","Flurry","#flurry","#flurry");
SkillBook::AddSkillToList("shove","Shove","#shove","#shove");

$AccessoryVar["#hide", $MiscInfo] = "Use near a wall or object to turn invisible. Hitting an enemy in the back with a piercing weapon will land a backstab. More Hiding will increase the rate you can travel and stay hidden. #w hidemoreinfo for more";
$AccessoryVar["#hidemoreinfo", $MiscInfo] = "While hidden, players do not update their \"scent\" marker. This can be useful for fooling other players trying to use #track on you, as they will be lead to the last scent location before you hid.";
$AccessoryVar["#bash",$MiscInfo] = "Increases the damage on your next hit with a Bludgeoning weapon. Also pushes your target back. Higher damage and bashing levels will push them farther.";
$AccessoryVar["#flurry",$MiscInfo] = "Useable with sword weapons.  Hits the target rapidly 3x at 65% strength.  Has a "@$SkillFlurryDelay@"s cooldown.";
$AccessoryVar["#shove",$MiscInfo] = "Pushes the target back slightly. Far weaker than a bash and scales with level. Useful if an afk player is blocking your path.";

SkillBook::AddSkillToList("castspell","Cast Spell","#cast","#cast <SpellName>");
SkillBook::AddSkillToList("attune","Attune to Item","#attune","#attune");
SkillBook::AddSkillToList("recharge","Recharge Item","#recharge","#recharge <mana>");

$AccessoryVar["#cast", $MiscInfo] = "Used to cast spells.  See the spell book in your TAB menu to see what spells you have available to cast currently.";
$AccessoryVar["#spell",$MiscInfo] = $AccessoryVar["#cast", $MiscInfo];
$AccessoryVar["#attune",$MiscInfo] = "If it can be attuned, attune to your currently equipped item (or weapon), unlocking bonuses or abilities. You can only attune to one item at a time. Any mana on the previous item will be lost.";
$AccessoryVar["#recharge",$MiscInfo] = "Recharge an equipped attuned weapon with mana.";

SkillBook::AddSkillToList("smithitem","Smith","#smith","#smith <ItemNameNoSpaces>");
SkillBook::AddSkillToList("mixitem","Mix","#mix","#mix <ItemNameNoSpaces>");
SkillBook::AddSkillToList("smeltitem","Smelt","#smelt","#smelt <ItemNameNoSpaces>");
SkillBook::AddSkillToList("cookitem","Cook","#cook","#cook <ItemNameNoSpaces>");

$AccessoryVar["#smith", $MiscInfo] = "Use to craft smithable item. Requires being by an anvil.";
$AccessoryVar["#mix", $MiscInfo] = "Use alchemy to craft an item.";
$AccessoryVar["#smelt", $MiscInfo] = "Smelt ore into an item. Requires being by an anvil.";
$AccessoryVar["#cook", $MiscInfo] = "Cook a food item. Food items give small bonuses.";

SkillBook::AddSkillToList("useitem","Use","#use","#use <ItemNameNoSpaces>");
SkillBook::AddSkillToList("dropcoins","Drop Coins","#dropcoins","#dropcoins <amount>");
SkillBook::AddSkillToList("createpack","Create Pack","#createpack","#createpack <item> <amnt> <item> <amnt>...");
SkillBook::AddSkillToList("sharepack","Share Pack","#sharepack","#sharepack <name> <pack# or *>");
SkillBook::AddSkillToList("unsharepack","Unshare Pack","#unsharepack","#unsharepack <name> <pack# or *>");

$AccessoryVar["#use", $MiscInfo] = "Use an item from your inventory or belt. This can be done to eat food, drink a potion, etc. This can also be done as a shortcut to equip weapons or armor. Very useful when paired with #set";
$AccessoryVar["#dropcoins", $MiscInfo] = "Drop coins on the ground.  Usage: \"#dropcoins amount\"";
$AccessoryVar["#createpack", $MiscInfo] = "Used to drop multiple items into a single pack on the ground from both your inventory and belt. Example: \"#createpack hatchet 6 smallrock 80 COINS 400 bluepotion 5\".";
$AccessoryVar["#sharepack", $MiscInfo] = "Used to allow another player to pick up your LCK locked pack.";
$AccessoryVar["#unsharepack", $MiscInfo] = "Removes a player from being able to pick up a pack you shared with them.";

SkillBook::AddSkillToList("setcamp","Camp","#camp","#camp");
SkillBook::AddSkillToList("uncamp","Pack Camp","#uncamp","#uncamp");
SkillBook::AddSkillToList("setanvil","Place Anvil","#anvil","#anvil");
SkillBook::AddSkillToList("unanvil","Pack Anvil","#unanvil","#unanvil");

$AccessoryVar["#camp", $MiscInfo] = "Use a tent from your inventory to setup a camp site in the Unknown. This site will act as a spawn point for you, provice a place to use #sleep, and will be saved with your character.";
$AccessoryVar["#uncamp", $MiscInfo] = "Use to pick up your nearby camp and store it in your inventory as a tent item.";
$AccessoryVar["#anvil", $MiscInfo] = "Allows you to place an anvil on the ground from your inventory for smithing.";
$AccessoryVar["#unanvil", $MiscInfo] = "Picks up one of your nearby anvils off the ground and returns it to your inventory.";

SkillBook::AddSkillToList("manaflare",$Ability::name[$Ability::index[manaflare]],"manaflare","#skill manaflare");
SkillBook::AddSkillToList("magebolt",$Ability::name[$Ability::index[magebolt]],"magebolt","#skill magebolt");
SkillBook::AddSkillToList("bladebolt",$Ability::name[$Ability::index[bladebolt]],"bladebolt","#skill bladebolt");
SkillBook::AddSkillToList("brace",$Ability::name[$Ability::index[brace]],"brace","#skill brace");
SkillBook::AddSkillToList("secondwind",$Ability::name[$Ability::index[secondwind]],"secondwind","#skill secondwind");
SkillBook::AddSkillToList("rage",$Ability::name[$Ability::index[rage]],"rage","#skill rage");
SkillBook::AddSkillToList("fade",$Ability::name[$Ability::index[fade]],"fade","#skill fade");
SkillBook::AddSkillToList("doublestrike",$Ability::name[$Ability::index[doublestrike]],"doublestrike","#skill doublestrike");
SkillBook::AddSkillToList("trueshot",$Ability::name[$Ability::index[trueshot]],"trueshot","#skill trueshot");

//Top of the tree
SkillBook::AddSkillMenuGroup("root","Root");

SkillBook::AddSkillMenuGroup("Class","Class Skills","root");
SkillBook::AddSkillMenuGroup("Combat","Combat","root");
SkillBook::AddSkillMenuGroup("Magic","Magic","root");
SkillBook::AddSkillMenuGroup("Command","Commands","root");
SkillBook::AddSkillMenuGroup("SenseHeading","Skill: Sense Heading","root");
SkillBook::AddSkillMenuGroup("Stealing","Skill: Stealing","root");

SkillBook::AddSkillMenuGroup("Bard","Bard","Class");
SkillBook::AddSkillMenuGroup("Cleric","Cleric","Class");
SkillBook::AddSkillMenuGroup("Druid","Druid","Class");
SkillBook::AddSkillMenuGroup("Fighter","Fighter","Class");
SkillBook::AddSkillMenuGroup("Mage","Mage","Class");
SkillBook::AddSkillMenuGroup("Paladin","Paladin","Class");
SkillBook::AddSkillMenuGroup("Ranger","Ranger","Class");
SkillBook::AddSkillMenuGroup("Thief","Thief","Class");

SkillBook::AddSkillToGroup("savechar","Command");
SkillBook::AddSkillToGroup("recall","Command");
SkillBook::AddSkillToGroup("setcmd","Command");
SkillBook::AddSkillMenuGroup("inventory","Inventory","Command");
SkillBook::AddSkillMenuGroup("talking","Talking","Command");
SkillBook::AddSkillMenuGroup("recovery","Recovery","Command");
SkillBook::AddSkillMenuGroup("info","Info","Command");
SkillBook::AddSkillMenuGroup("crafting","Craft","Command");
SkillBook::AddSkillMenuGroup("camping","Camping","Command");

SkillBook::FillSkillGroup("Bard",$MinClass,"Bard");
SkillBook::FillSkillGroup("Bard",$MinGroup,$ClassGroup[Bard]);
SkillBook::FillSkillGroup("Cleric",$MinClass,"Cleric");
SkillBook::FillSkillGroup("Cleric",$MinGroup,$ClassGroup[Cleric]);
SkillBook::FillSkillGroup("Druid",$MinClass,"Druid");
SkillBook::FillSkillGroup("Druid",$MinGroup,$ClassGroup[Druid]);
SkillBook::FillSkillGroup("Fighter",$MinClass,"Fighter");
SkillBook::FillSkillGroup("Fighter",$MinGroup,$ClassGroup[Fighter]);
SkillBook::FillSkillGroup("Mage",$MinClass,"Mage");
SkillBook::FillSkillGroup("Mage",$MinGroup,$ClassGroup[Mage]);
SkillBook::FillSkillGroup("Paladin",$MinClass,"Paladin");
SkillBook::FillSkillGroup("Paladin",$MinGroup,$ClassGroup[Paladin]);
SkillBook::FillSkillGroup("Ranger",$MinClass,"Ranger");
SkillBook::FillSkillGroup("Ranger",$MinGroup,$ClassGroup[Ranger]);
SkillBook::FillSkillGroup("Thief",$MinClass,"Thief");
SkillBook::FillSkillGroup("Thief",$MinGroup,$ClassGroup[Thief]);


SkillBook::FillSkillGroup("Combat",$SkillBashing);
SkillBook::AddSkillToGroup("flurry","Combat");
SkillBook::FillSkillGroup("SenseHeading",$SkillSenseHeading);
SkillBook::FillSkillGroup("Stealing",$SkillStealing);

SkillBook::AddSkillToGroup("talksay","talking");
SkillBook::AddSkillToGroup("talkshout","talking");
SkillBook::AddSkillToGroup("talkwhisper","talking");
SkillBook::AddSkillToGroup("talksglobal","talking");
SkillBook::AddSkillToGroup("talktell","talking");
SkillBook::AddSkillToGroup("talkr","talking");
SkillBook::AddSkillToGroup("talkparty","talking");
SkillBook::AddSkillToGroup("talkgroup","talking");
SkillBook::AddSkillToGroup("talkzone","talking");

SkillBook::AddSkillToGroup("sleep","recovery");
SkillBook::AddSkillToGroup("heal","recovery");
SkillBook::AddSkillToGroup("rest","recovery");
SkillBook::AddSkillToGroup("wake","recovery");

SkillBook::AddSkillToGroup("getinfo","info");
SkillBook::AddSkillToGroup("setinfo","info");
SkillBook::AddSkillToGroup("whatis","info");
SkillBook::AddSkillToGroup("craftinfo","info");

SkillBook::AddSkillToGroup("smithitem","crafting");
SkillBook::AddSkillToGroup("mixitem","crafting");
SkillBook::AddSkillToGroup("smeltitem","crafting");
SkillBook::AddSkillToGroup("cookitem","crafting");

SkillBook::AddSkillToGroup("useitem","inventory");
SkillBook::AddSkillToGroup("dropcoins","inventory");
SkillBook::AddSkillToGroup("createpack","inventory");
SkillBook::AddSkillToGroup("sharepack","inventory");
SkillBook::AddSkillToGroup("unsharepack","inventory");

SkillBook::AddSkillToGroup("castspell","Magic");
SkillBook::AddSkillToGroup("attune","Magic");
SkillBook::AddSkillToGroup("recharge","Magic");

SkillBook::AddSkillToGroup("setcamp","Camping");
SkillBook::AddSkillToGroup("uncamp","Camping");
SkillBook::AddSkillToGroup("setanvil","Camping");
SkillBook::AddSkillToGroup("unanvil","Camping");

function MenuViewSkillBook(%clientId,%groupLabel,%page)
{
    if(%page == "")
        %page = 1;
    %header = "Skill Types:";
    if(%groupLabel != "root")
        %header = $SkillBook::SkillGroup[%groupLabel,Name] @":";
	Client::buildMenu(%clientId, %header, "ViewSkillBook", true);
    
    %num = $SkillBook::SkillGroup[%groupLabel,NumEntries];
    %menuULB = BeltMenu::GetUpperLowerBounds(%num,%page);

    %numFullPages = getWord(%menuULB,0);
    %lb = getWord(%menuULB,1);
    %ub = getWord(%menuULB,2);
    %x = %lb - 1;
	for(%i = %lb; %i <= %ub; %i++)
	{
        echo(%x);
        %entry = $SkillBook::SkillGroup[%groupLabel,Entry,%x];
        echo(%entry);
        %disp = "";
        %type = $SkillBook::Type[%entry];
        if(%type == $SkillBook::TypeGroup)
        {
            %disp = $SkillBook::SkillGroup[%entry,Name] @"...";
        }
        else if(%type == $SkillBook::TypeSkill)
        {
            %disp = $SkillBook::Skill[%entry,Name] @ " ("@$SkillBook::Skill[%entry,IDLabel]@")";
            echo(%disp);
        }
        Client::addMenuItem(%clientId, %cnt++ @ %disp, %entry @" "@%groupLabel);
        %x++;
    }
    
    if(%page == 1)
	{
		if(%num > 6) Client::addMenuItem(%clientId, "nNext >>", "page "@%groupLabel@" " @ %page+1);
        if(%groupLabel == "root")
            Client::addMenuItem(%clientId, "bDone", "done");
        else
            Client::addMenuItem(%clientId, "bBack", "back "@%groupLabel);
	}
	else if(%page == %numFullPages+1)
	{
		Client::addMenuItem(%clientId, "p<< Prev", "page " @%groupLabel@" "@ %page-1);
		if(%groupLabel == "root")
            Client::addMenuItem(%clientId, "bDone", "done");
        else
            Client::addMenuItem(%clientId, "bBack", "back "@%groupLabel);
	}
	else
	{
		Client::addMenuItem(%clientId, "nNext >>", "page " @%groupLabel@" "@ %page+1);
		Client::addMenuItem(%clientId, "p<< Prev", "page " @%groupLabel@" "@ %page-1);
	}
    return;
}

function processMenuViewSkillBook(%clientId, %opt)
{
    %option = getWord(%opt,0);
    %label = getWord(%opt,1);
    if(%option == "page")
    {
        %pageNum = getWord(%opt,2);
        MenuViewSkillBook(%clientId,%label,%pageNum);
        return;
    }
    else if(%option == "back")
    {
        MenuViewSkillBook(%clientId,$SkillBook::SkillGroup[%label,Parent],1);
        return;
    }
    else if(%option != "done")
    {
        %type = $SkillBook::Type[%option];
        if(%type == $SkillBook::TypeGroup)
        {
            MenuViewSkillBook(%clientId,%option,1);
        }
        else if(%type == $SkillBook::TypeSkill)
        {
            MenuViewSkill(%clientId,%option,%label);
        }
    }
}

function MenuViewSkill(%clientId,%label,%prevGroup)
{
    %header = $SkillBook::Skill[%label,Name] @":";
	Client::buildMenu(%clientId, %header, "ViewSkill", true);
    
    Client::addMenuItem(%clientId, %cnt++ @ "View Info", "info "@%label@" "@%prevGroup);
    Client::addMenuItem(%clientId, %cnt++ @ "Cmd: "@$SKillBook::Skill[%label,Command], "cmd "@%label@" "@%prevGroup);
    Client::addMenuItem(%clientId, "bBack", "back "@%label@" "@%prevGroup);
}

function processMenuViewSkill(%clientId,%opt)
{
    %option = getWord(%opt,0);
    %label = getWord(%opt,1);
    %prevGrp = getWord(%opt,2);
    if(%option == "info")
    {
        %msg = WhatIs($SkillBook::Skill[%label,IDLabel]);
		bottomprint(%clientId, %msg, floor(String::len(%msg) / 20));
        MenuViewSkill(%clientId,%label,%prevGrp);
    }
    else if(%option == "cmd")
    {
        Client::sendMessage(%clientId,$MsgWhite,"Command: "@ $SKillBook::Skill[%label,Command]);
        MenuViewSkill(%clientId,%label,%prevGrp);
    }
    else if(%option == "back")
    {
    
        MenuViewSkillBook(%clientId,%prevGrp,1);
    }
}