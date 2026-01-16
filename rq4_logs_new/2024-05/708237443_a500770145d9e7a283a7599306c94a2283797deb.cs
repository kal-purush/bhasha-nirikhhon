//Script used to create the new gui stuff for the client.
//This script isn't run. It's just here for documentation
//Tribes GUI editor sucks, so it's easier to add elements via script and then save the gui
function SetupNewGui()
{
    focusClient();
    %playGui = NameToID("playGui");
    
    %gctl[1] = newObject("DmgTextArea1",SimGui::Control);
    %gctl[2] = newObject("DmgTextArea2",SimGui::Control);
    %gctl[3] = newObject("DmgTextArea3",SimGui::Control);
    %gctl[4] = newObject("DmgTextArea4",SimGui::Control);
    %gctl[5] = newObject("DmgTextArea5",SimGui::Control);
    %gctl[6] = newObject("ZoneTextArea",SimGui::Control);
    
    Control::setPosition("DmgTextArea1",353,225);
    Control::setExtent("DmgTextArea1",318,318);
    Control::setPosition("DmgTextArea2",353,225);
    Control::setExtent("DmgTextArea2",318,318);
    Control::setPosition("DmgTextArea3",353,225);
    Control::setExtent("DmgTextArea3",318,318);
    Control::setPosition("DmgTextArea4",353,225);
    Control::setExtent("DmgTextArea4",318,318);
    Control::setPosition("DmgTextArea5",353,225);
    Control::setExtent("DmgTextArea5",318,318);
    Control::setPosition("ZoneTextArea",353,120); //59,75 with ResizeRight ResizeBottom
    Control::setExtent("ZoneTextArea",600,27);
    
    for(%i = 1; %i <= 5; %i++)
    {
        %atk = newObject("ATKText"@%i,FearGuiFormattedText);
        Control::setExtent("ATKText"@%i,318,6);
        Control::setPosition("ATKText"@%i, 0, 107);
        addToSet(%gctl[%i],%atk);
        addToSet(%playGui,%gctl[%i]);
    }
    
    %zz = newObject("ZONEText",FearGuiFormattedText);
    Control::setExtent("ZONEText",600,6);
    Control::setPosition("ZONEText",0,0);
    addToSet(%gctl[6],%zz);
    addToSet(%playGui,%gctl[6]);
    
}

function TestText(%txt)
{
    %num = 1;
    Control::setValue("ATKText"@%num, %text);
    
}