using Common.Dtos;
using Web.User.Models;

namespace Web.User.Helpers
{
    public class Mapper
    {
        public void Map(ApplicationUserResponseDto userResponseDto, ProfileModel profileModel)
        {
            if (userResponseDto == null) { return; }
            if (profileModel == null) { 
                profileModel = new ProfileModel();
            }
            profileModel.Email = userResponseDto.Email;
            profileModel.UserName = userResponseDto.UserName;
        }
    }
}