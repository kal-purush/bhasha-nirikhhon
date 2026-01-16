class Location
{
    public int Location_ID;
    public string Location_Name;
    public string Location_Description;
    public string Location_Unlock_Requirements;
    public bool Location_Unlocked;
    
    public Location(int Location_ID,  string Location_Name,  string Location_Description, string Location_Unlock_Requirements, string Location_Unlocked)
    {
        this.Location_ID = Location_ID;
        this.Location_Name = Location_Name;
        this.Location_Description = Location_Description;
        
        this.Location_Unlock_Requirements = Location_Unlock_Requirements;
        if (Location_Unlocked is null)
        {
            this.Location_Unlocked = true;
        }
        else if (Location_Unlocked == "n")
        {
            this.Location_Unlocked = false;
        }
    }

}