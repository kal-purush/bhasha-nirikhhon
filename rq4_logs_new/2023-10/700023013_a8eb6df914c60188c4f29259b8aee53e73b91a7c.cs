using MediatR;

namespace Application.Users.Features.GetUserPhoto;

public class GetUserPhotoHandler : IRequestHandler<GetUserPhotoRequest, GetUserPhotoResponse>
{
    public async Task<GetUserPhotoResponse> Handle(GetUserPhotoRequest request, CancellationToken cancellationToken)
    {
        if (request.UserId != request.LoggedUserId) 
            throw new UnauthorizedAccessException("Given user id is not valid");
        
        string userPhotoPath = $"Photos/user-{request.UserId}.png";
        byte[] userPhoto = await File.ReadAllBytesAsync(userPhotoPath, cancellationToken);

        return new GetUserPhotoResponse { UserPhoto = userPhoto };
    }
}