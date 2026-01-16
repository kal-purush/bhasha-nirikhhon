using backend_carhelp.Workshop_management.Domain.Model.Commands;

namespace backend_carhelp.Workshop_management.Domain.Model.Aggregates;

public class Advertasing
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string ImageUrl{ get; set; }
    public string Slogan{ get; set; }
    public string Message{ get; set; }
    public int WorkshopId{ get; set; }

    public Advertasing()
    {
        Name= string.Empty;
        ImageUrl= string.Empty;
        Slogan= string.Empty;
        Message = string.Empty;
        WorkshopId = 0;
    }
    
    public Advertasing(string name, string imageUrl, string slogan, string message, int workshopId) {
        Name = name;
        ImageUrl = imageUrl;
        Slogan = slogan;
        Message = message;
        WorkshopId = workshopId;
    }

    public Advertasing(CreateAdvertasingCommand createAdvertasingCommand)
    {
        Name = createAdvertasingCommand.Name;
        ImageUrl = createAdvertasingCommand.ImageUrl;
        Slogan = createAdvertasingCommand.Slogan;
        Message = createAdvertasingCommand.Message;
        WorkshopId = createAdvertasingCommand.WorkshopId;
    }
}