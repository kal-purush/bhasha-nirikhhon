using CSharpFunctionalExtensions;
using PetFamily.Domain.Shared;

namespace PetFamily.Domain.Entities.Pets;

public class PetPhoto : Shared.Entity<PetPhotoId>
{
    private PetPhoto()
    {
        
    }
    
    private PetPhoto(PetPhotoId id, string path, bool isMain) : base(id)
    {
        Path = path;
        IsMain = isMain;
    }
    
    public string Path { get; private set; }
    
    public bool IsMain { get; private set; }

    public static Result<PetPhoto, Error> Create(PetPhotoId id, string path, bool isMain)
    {
        if (string.IsNullOrWhiteSpace(path))
            return Errors.General.ValueIsInvalid(nameof(path));
        
        var petPhoto = new PetPhoto(id, path, isMain);

        return petPhoto;
    }
}