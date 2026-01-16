using ReptileTracker.Commons;

namespace ReptileTracker.Shedding.Errors;

public class SheddingErrors
{
    public static readonly Error CantSave = new Error("Shedding.CantSave", "Cant add this shedding");
    public static readonly Error CantDelete = new Error("Shedding.CantDelete", "Cant delete this shedding event");
    public static readonly Error NotFound = new Error("Shedding.NotFound", "shedding not found.");
    public static readonly Error CantUpdate = new Error("Shedding.CantUpdate", "Cant update shedding event");
    public static readonly Error NoSheddingHistory = new Error("Feeeding.NoHistory", "No shedding history fouynd");

    public static readonly Error EventlistNotFound =
        new Error("Shedding.EventlistNotFound", "Cant find the event list"); 
}