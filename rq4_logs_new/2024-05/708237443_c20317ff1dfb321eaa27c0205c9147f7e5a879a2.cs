function PlaceMarkerInGroupAtLOS(%clientId,%name,%grpId)
{
    %pl = Client::getOwnedObject(%clientId);
    if(Gamebase::getLOSInfo(%pl,500))
    {
        %obj = newObject(%name,Marker,PathMarker);
        Gamebase::setPosition(%obj,$los::position);
        Gamebase::setRotation(%obj,Gamebase::getRotation(%clientId));
        addToSet(%grpId,%obj);
        
        Client::sendMessage(%clientId,$MsgWhite,"Placed marker ("@ %name @") at "@ $los::position @" in Group "@ Object::getName(%grpId));
    }
    else
        Client::sendMessage(%clientId,$MsgRed,"No LOS found");
}