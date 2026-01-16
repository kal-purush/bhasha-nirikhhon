using Shufl.API.Infrastructure.Enums;
using Shufl.API.Infrastructure.Exceptions;
using Shufl.API.UploadModels.Group;
using Shufl.Domain.Entities;
using Shufl.Domain.Repositories.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shufl.API.Models.Group
{
    public static class GroupPlaylistRatingModel
    {
        public static async Task<GroupPlaylistRating> CreateNewGroupPlaylistRatingAsync(
            string groupIdentifier,
            string groupPlaylistIdentifier,
            GroupPlaylistRating groupPlaylistRating,
            Guid userId,
            IRepositoryManager repositoryManager)
        {
            groupIdentifier = groupIdentifier.ToUpperInvariant();
            groupPlaylistIdentifier = groupPlaylistIdentifier.ToUpperInvariant();

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
                        var groupPlaylist = await repositoryManager.GroupPlaylistRepository.GetByIdentifierAndGroupIdAsync(
                            groupPlaylistIdentifier,
                            group.Id);

                        if (groupPlaylist != null)
                        {
                            var existingGroupPlaylistRating = (await repositoryManager.GroupPlaylistRatingRepository.FindAsync(gsr =>
                                gsr.GroupPlaylistId == groupPlaylist.Id &&
                                gsr.CreatedBy == userId)).FirstOrDefault();

                            if (existingGroupPlaylistRating != null)
                            {
                                existingGroupPlaylistRating.OverallRating = groupPlaylistRating.OverallRating;
                                existingGroupPlaylistRating.Comment = groupPlaylistRating.Comment;
                                groupPlaylistRating.LastUpdatedOn = DateTime.Now;
                                groupPlaylistRating.LastUpdatedBy = userId;

                                return await repositoryManager.GroupPlaylistRatingRepository.UpdateAsync(existingGroupPlaylistRating);
                            }
                            else
                            {
                                groupPlaylistRating.GroupPlaylistId = groupPlaylist.Id;
                                groupPlaylistRating.CreatedOn = DateTime.Now;
                                groupPlaylistRating.CreatedBy = userId;
                                groupPlaylistRating.LastUpdatedOn = DateTime.Now;
                                groupPlaylistRating.LastUpdatedBy = userId;

                                return await repositoryManager.GroupPlaylistRatingRepository.AddAsync(groupPlaylistRating);
                            }
                        }
                        else
                        {
                            throw new InvalidTokenException(InvalidTokenType.TokenNotFound, "The requested Group Playlist was not found");
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

        public static async Task<GroupPlaylistRating> EditGroupPlaylistRatingAsync(
            Guid groupPlaylistRatingId,
            GroupPlaylistRatingUploadModel groupPlaylistRatingUploadModel,
            Guid userId,
            IRepositoryManager repositoryManager)
        {
            try
            {
                var groupPlaylistRating = await repositoryManager.GroupPlaylistRatingRepository.GetByIdAsync(groupPlaylistRatingId);

                if (groupPlaylistRating != null)
                {
                    if (groupPlaylistRating.CreatedBy == userId)
                    {
                        groupPlaylistRating.OverallRating = groupPlaylistRatingUploadModel.OverallRating;
                        groupPlaylistRating.Comment = groupPlaylistRatingUploadModel.Comment;
                        groupPlaylistRating.LastUpdatedOn = DateTime.Now;
                        groupPlaylistRating.LastUpdatedBy = userId;

                        await repositoryManager.GroupPlaylistRatingRepository.UpdateAsync(groupPlaylistRating);

                        return groupPlaylistRating;
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
                    throw new InvalidTokenException(InvalidTokenType.TokenNotFound, "The requested Group Playlist Rating was not found");
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static async Task DeleteGroupPlaylistRatingAsync(
            Guid groupPlaylistRatingId,
            Guid userId,
            IRepositoryManager repositoryManager)
        {
            try
            {
                var groupPlaylistRating = await repositoryManager.GroupPlaylistRatingRepository.GetByIdAsync(groupPlaylistRatingId);

                if (groupPlaylistRating != null)
                {
                    if (groupPlaylistRating.CreatedBy == userId)
                    {
                        await repositoryManager.GroupPlaylistRatingRepository.RemoveAsync(groupPlaylistRating);
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
                    throw new InvalidTokenException(InvalidTokenType.TokenNotFound, "The requested Group Playlist Rating was not found");
                }
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}