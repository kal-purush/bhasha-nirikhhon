$RPGItem::InvItemType = 0;
$RPGItem::BeltItemType = 1;

// Only run this function once after startup.
function RPGItem::BuildInventoryItemList()
{
    %max = getNumItems();
	for(%i = 0; %i < %max; %i++)
	{
        %item = getItemData(%i);
        $RPGItem::InvItem[%item,Name] = %item.description;
        $RPGItem::InvItem[%item,ClassName] = %item.className;
        $RPGItem::InvItem[%item,Index] = %i;
        $RPGItem::IndexToInvItem[%i] = %item;
    }
}

function RPGItem::setItemCount(%clientId,%item,%amt)
{
    %type = RPGItem::getItemInternalType(%item);
    %ret = 0;
    if(%type == $RPGItem::InvItemType)
    {
        %ret = Player::setItemCount(%clientId,%item,%amt);
        RPGItem::updateItemList(%clientId,%item);
    }
    else if(%type == $RPGItem::BeltItemType)
    {
        %ret = Belt::SetItemCount(%item,%amt);
    }
    return %ret;
}

function RPGItem::incItemCount(%clientId,%item,%amt,%showmsg)
{
    %type = RPGItem::getItemInternalType(%item);
    
    if(%type == $RPGItem::InvItemType)
    {
        %ret = Item::giveItem(Client::getOwnedObject(%clientId), %item, %amt, %showmsg);
        RPGItem::updateItemList(%clientId,%item);
    }
    else if(%type == $RPGItem::BeltItemType)
    {
        Belt::GiveThisStuff(%clientid, %item, %amt, %showmsg);
        %ret = Belt::HasThisStuff(%clientid,%item);
    }
    return %ret;
}

function RPGItem::updateItemList(%clientId,%item)
{
    %invList = fetchData(%clientId,"InvItemList");
    %found = Word::FindWord(%invList,%item,",") != -1;
    if(Player::getItemCount(%clientId,%item) == 0)
    {
        if(%found)
        {
            echo("Change");
            %invList = String::replace(%invList,%item@",","");
            echo(%invList);
            if(String::right(%invList,1) != ",")
                %invList = %invList @",";
            storeData(%clientId,"InvItemList",%invList);
        }
    }
    else
    {
        if(!%found)
        {
            %invList = %invList @ %item @",";
            storeData(%clientId,"InvItemList",%invList);
        }
    }
}

function RPGItem::getItemCount(%clientId,%item)
{
    %type = RPGItem::getItemInternalType(%item);
    %ret = "";
    if(%type == $RPGItem::InvItemType)
    {
        %ret = Player::getItemCount(%clientId,%item);
    }
    else if(%type == $RPGItem::BeltItemType)
    {
        %ret = Belt::HasThisStuff(%clientid,%item);
    }
    return %ret;
}

function RPGItem::decItemCount(%clientId,%item,%amt,%showmsg)
{
    %type = RPGItem::getItemInternalType(%item);
    
    if(%type == $RPGItem::InvItemType)
    {
        %ret = Item::takeItem(Client::getOwnedObject(%clientId), %item, %amt, %showmsg);
        RPGItem::updateItemList(%clientId,%item);
    }
    else if(%type == $RPGItem::BeltItemType)
    {
        Belt::TakeThisStuff(%clientid, %item, %amt, %showmsg);
        %ret = Belt::HasThisStuff(%clientid,%item);
    }
    return %ret;
}

function RPGItem::isBeltItem(%item)
{
    return isBeltItem(%item);
}

function RPGItem::isInventoryItem(%item)
{
    return $RPGItem::InvItem[%item,Index] != "";
}

function RPGItem::getItemInternalType(%item)
{
    if(RPGItem::isInventoryItem(%item))
        return $RPGItem::InvItemType;
    else if(RPGItem::isBeltItem(%item))
        return $RPGItem::BeltItemType;
    else
        return -1;
}