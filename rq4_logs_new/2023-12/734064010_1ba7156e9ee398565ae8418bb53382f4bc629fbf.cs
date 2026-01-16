using ReptileTracker.Commons;

namespace ReptileTracker.Animal.Errors;

public class LengthErrors
{
    public static readonly Error CantSave = new Error("Length.CantSave", "Cant add this length");
    public static readonly Error CantDelete = new Error("Length.CantDelete", "Cant delete this length event");
    public static readonly Error NotFound = new Error("Length.NotFound", "length not found.");
    public static readonly Error CantUpdate = new Error("Length.CantUpdate", "Cant update length event");
    public static readonly Error NoLengthHistory = new Error("Length.NoHistory", "No length history found");

    public static readonly Error EventlistNotFound =
        new Error("Length.EventlistNotFound", "Cant find the event list");
}