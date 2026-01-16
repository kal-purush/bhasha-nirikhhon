// WIP
$Attunement::SlotWeapon = 0;
$Attunement::SlotArmor = 1;
$Attunement::SlotNecklace = 2;

$Attunement::Slot[$Attunement::SlotWeapon,DataId] = "attunedWeapon";
$Attunement::Slot[$Attunement::SlotWeapon,ManaDataId] = "attunedWeaponMana";

$Attunement::Slot[$Attunement::SlotArmor,DataId] = "attunedArmor";
$Attunement::Slot[$Attunement::SlotArmor,ManaDataId] = "attunedArmorMana";

$Attunement::Slot[$Attunement::SlotNecklace,DataId] = "attunedNecklace";
$Attunement::Slot[$Attunement::SlotNecklace,ManaDataId] = "attunedNecklaceMana";

$Attunement::AttunableItem[NoviceStaff,MaxMana] = 50;
$Attunement::AttunableItem[NoviceStaff,ManaCost] = 1;
$Attunement::AttunableItem[NoviceStaff,AttunementTime] = 2; //Seconds
$Attunement::AttunableItem[NoviceStaff,AttunementCost] = 10;
$Attunement::AttunableItem[NoviceStaff,Slot] = $Attunement::SlotWeapon;


function Attunement::IsAttunableItem(%item)
{
    return $Attunement::AttunableItem[%item,MaxMana] != "";
}

function Attunement::getEquippedItemBySlot(%clientId,%slot)
{
    if(%slot == $Attunement::SlotWeapon)
    {
        Player::getMountedItem(Client::getOwnedObject(%clientId),$WeaponSlot);
    }
    else if(%slot == $Attunement::SlotArmor)
    {
        return getWord(GetAccessoryList(%clientId, 13, -1),0); //There should only be 1 armor equpped, but just in case...
    }
    else if(%slot == $Attunement::SlotNecklace)
    {
        return Player::GetEquippedBeltItem(%clientId,"neck");
    }
}

function Attunement::setAttunedItem(%clientId,%item)
{
    %slot = $Attunement::AttunableItem[%item,Slot];
    if(%slot != "")
    {
        storeData(%clientId,$Attunement::Slot[%slot,DataId];
        return true;
    }
    return false;
}

function Attunement::getAttunedItem(%clientId,%slot)
{
    return fetchData(%clientId,$Attunement::Slot[%slot,DataId]);
}

function Attunement::getMana(%clientId,%slot)
{
    return fetchData(%clientId,$Attunement::Slot[%slot,ManaDataId]);
}

function Attunement::setMana(%clientId,%slot,%mana)
{
    return storeData(%clientId,$Attunement::Slot[%slot,ManaDataId],%amnt);
}

function Attunement::addMana(%clientId,%slot,%mana)
{
    return storeData(%clientId,$Attunement::Slot[%slot,ManaDataId],%amnt,"inc");
}

function Attunement::useMana(%clientId,%slot,%mana)
{
    %cur = fetchData(%clientId,$Attunement::Slot[%slot,ManaDataId]);
    if(%mana >= %cur)
    {
        storeData(%clientId,$Attunement::Slot[%slot,ManaDataId],%mana,"dec");
        return true;
    }

    return false;
}