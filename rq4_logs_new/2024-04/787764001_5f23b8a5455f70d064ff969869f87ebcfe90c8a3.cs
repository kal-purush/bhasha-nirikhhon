using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TechTitans.Repositories;
using TechTitans.Models;
using Dapper;

namespace TechTitans.Repositories
{
    internal class UserRepository : Repository<SongBasicDetails>
    {
            public SongBasicInfo SongBasicDetailsToSongBasicInfo(SongBasicDetails songBasicDetails)
            {
                var artistId = songBasicDetails.ArtistId;
                var cmd = new StringBuilder();
                cmd.Append("SELECT name FROM AuthorDetails WHERE artist_id = @artistId");
                var artistName = _connection.Query<string>(cmd.ToString(), new { artistId }).FirstOrDefault();
                return new SongBasicInfo
                {
                    SongId = songBasicDetails.SongId,
                    Name = songBasicDetails.Name,
                    Genre = songBasicDetails.Genre,
                    Subgenre = songBasicDetails.Subgenre,
                    Artist = artistName,
                    Language = songBasicDetails.Language,
                    Country = songBasicDetails.Country,
                    Album = songBasicDetails.Album,
                    Image = songBasicDetails.Image
                };
            }
        }
    }
