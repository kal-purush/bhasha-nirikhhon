using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TechTitans.Models;

namespace TechTitans.Repositories
{
    /// <summary>
    /// Defines the contract for a repository that manages user song data.
    /// </summary>
    public interface IUserSongRepository : IRepository<SongDataBaseModel>
    {
        /// <summary>
        /// Converts a song database model to a simplified song information model,
        /// including retrieving the artist's name.
        /// </summary>
        /// <param name="songBasicDetails">The song database model to convert.</param>
        /// <returns>A simplified song information model with the artist's name included.</returns>
        public SongBasicInformation ConvertSongDataBaseModelToSongInfo(SongDataBaseModel songBasicDetails);
    }
}