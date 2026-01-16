using Shufl.API.Infrastructure.Enums;
using Shufl.API.Infrastructure.Exceptions;
using Shufl.API.UploadModels.Group;
using Shufl.Domain.Entities;
using Shufl.Domain.Repositories.Interfaces;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Shufl.API.Models.Group
{
    public static class GroupAlbumRatingModel
    {
        public static async Task<GroupAlbumRating> CreateNewGroupAlbumRatingAsync(
            string groupIdentifier,
            string groupAlbumIdentifier,
            GroupAlbumRating groupAlbumRating,
            Guid userId,
            IRepositoryManager repositoryManager)
        {
            groupIdentifier = groupIdentifier.ToUpperInvariant();
            groupAlbumIdentifier = groupAlbumIdentifier.ToUpperInvariant();

            try
            {
                var group = await repositoryManager.GroupRepository.GetByIdentifierAsync(groupIdentifier);

                if (group != null)
                {
                    var isUserMemberOfGroup = await GroupMemberModel.CheckGroupMemberExistsAsync(
                           groupIdentifier,
                           userId,
                           repositoryManager);

                    if (isUserMemberOfGroup)
                    {
                        var groupAlbum = await repositoryManager.GroupAlbumRepository.GetByIdentifierAndGroupIdAsync(
                            groupAlbumIdentifier,
                            group.Id);

                        if (groupAlbum != null)
                        {
                            var existingGroupAlbumRating = (await repositoryManager.GroupAlbumRatingRepository.FindAsync(gsr =>
                                gsr.GroupAlbumId == groupAlbum.Id &&
                                gsr.CreatedBy == userId)).FirstOrDefault();

                            if (existingGroupAlbumRating != null)
                            {
                                existingGroupAlbumRating.OverallRating = groupAlbumRating.OverallRating;
                                existingGroupAlbumRating.LyricsRating = groupAlbumRating.LyricsRating;
                                existingGroupAlbumRating.VocalsRating = groupAlbumRating.VocalsRating;
                                existingGroupAlbumRating.InstrumentalsRating = groupAlbumRating.InstrumentalsRating;
                                existingGroupAlbumRating.StructureRating = groupAlbumRating.StructureRating;
                                existingGroupAlbumRating.Comment = groupAlbumRating.Comment;
                                existingGroupAlbumRating.LastUpdatedOn = DateTime.Now;
                                existingGroupAlbumRating.LastUpdatedBy = userId;

                                return await repositoryManager.GroupAlbumRatingRepository.UpdateAsync(existingGroupAlbumRating);
                            }
                            else
                            {
                                groupAlbumRating.GroupAlbumId = groupAlbum.Id;
                                groupAlbumRating.CreatedOn = DateTime.Now;
                                groupAlbumRating.CreatedBy = userId;
                                groupAlbumRating.LastUpdatedOn = DateTime.Now;
                                groupAlbumRating.LastUpdatedBy = userId;

                                return await repositoryManager.GroupAlbumRatingRepository.AddAsync(groupAlbumRating);
                            }
                        }
                        else
                        {
                            throw new InvalidTokenException(InvalidTokenType.TokenNotFound, "The requested Group Album was not found");
                        }
                    }
                    else
                    {
                        throw new UserNotGroupMemberException(
                            "You are not able to perform this action",
                            "You are not able to perform this action");
                    }
                }
                else
                {
                    throw new InvalidTokenException(InvalidTokenType.TokenNotFound, "The requested Group was not found");
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static async Task<GroupAlbumRating> EditGroupAlbumRatingAsync(
            Guid groupAlbumRatingId,
            GroupAlbumRatingUploadModel groupAlbumRatingUploadModel,
            Guid userId,
            IRepositoryManager repositoryManager)
        {
            try
            {
                var groupAlbumRating = await repositoryManager.GroupAlbumRatingRepository.GetByIdAsync(groupAlbumRatingId);

                if (groupAlbumRating != null)
                {
                    if (groupAlbumRating.CreatedBy == userId)
                    {
                        groupAlbumRating.OverallRating = groupAlbumRatingUploadModel.OverallRating;
                        groupAlbumRating.LyricsRating = groupAlbumRatingUploadModel.LyricsRating;
                        groupAlbumRating.VocalsRating = groupAlbumRatingUploadModel.VocalsRating;
                        groupAlbumRating.InstrumentalsRating = groupAlbumRatingUploadModel.InstrumentalsRating;
                        groupAlbumRating.StructureRating = groupAlbumRatingUploadModel.StructureRating;
                        groupAlbumRating.Comment = groupAlbumRatingUploadModel.Comment;
                        groupAlbumRating.LastUpdatedOn = DateTime.Now;
                        groupAlbumRating.LastUpdatedBy = userId;

                        await repositoryManager.GroupAlbumRatingRepository.UpdateAsync(groupAlbumRating);

                        return groupAlbumRating;
                    }
                    else
                    {
                        throw new UserForbiddenException(
                            "You are not allowed to perform this action",
                            "You are not allowed to perform this action");
                    }
                }
                else
                {
                    throw new InvalidTokenException(InvalidTokenType.TokenNotFound, "The requested Group Album Rating was not found");
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static async Task DeleteGroupAlbumRatingAsync(
            Guid groupAlbumRatingId,
            Guid userId,
            IRepositoryManager repositoryManager)
        {
            try
            {
                var groupAlbumRating = await repositoryManager.GroupAlbumRatingRepository.GetByIdAsync(groupAlbumRatingId);

                if (groupAlbumRating != null)
                {
                    if (groupAlbumRating.CreatedBy == userId)
                    {
                        await repositoryManager.GroupAlbumRatingRepository.RemoveAsync(groupAlbumRating);
                    }
                    else
                    {
                        throw new UserForbiddenException(
                            "You are not allowed to perform this action",
                            "You are not allowed to perform this action");
                    }
                }
                else
                {
                    throw new InvalidTokenException(InvalidTokenType.TokenNotFound, "The requested Group Album Rating was not found");
                }
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}