using DGFScouting.Data;
using DGFScouting.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace DGFScouting.Controllers
{
    public class CommittedPlayersController : ApiController
    {
        // Enable database operations
        RecruitsDbContext recruitsDbContext = new RecruitsDbContext();
        
        // GET: api/CommittedPlayers?sort=
        public IHttpActionResult Get(string sort)
        {
            IQueryable<CommittedPlayer> committedPlayers;
            switch (sort)
            {
                case "desc":
                    committedPlayers = recruitsDbContext.CommittedPlayers.OrderByDescending(q => q.JerseyNumber);
                    break;
                case "asc":
                    committedPlayers = recruitsDbContext.CommittedPlayers.OrderBy(q => q.JerseyNumber);
                    break;
                default:
                    committedPlayers = recruitsDbContext.CommittedPlayers;
                    break;
            }

            return Ok(committedPlayers);
        }

        // POST: api/CommittedPlayers
        public IHttpActionResult Post(CommittedPlayer committedPlayer)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            recruitsDbContext.CommittedPlayers.Add(committedPlayer);
            recruitsDbContext.SaveChanges();
            return StatusCode(HttpStatusCode.Created);
        }

        // PUT: api/CommittedPlayers/5
        public IHttpActionResult Put(int id, [FromBody]CommittedPlayer committedplayer)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var entity = recruitsDbContext.CommittedPlayers.FirstOrDefault(q => q.Id == id);

            if (entity == null)
            {
                return BadRequest("No record found matching this ID.");
            }

            entity.FirstName = committedplayer.FirstName;
            entity.LastName = committedplayer.LastName;
            entity.Email = committedplayer.Email;
            entity.PhoneNumber = committedplayer.PhoneNumber;
            entity.JerseyNumber = committedplayer.JerseyNumber;
            entity.CommitmentDate = committedplayer.CommitmentDate;

            recruitsDbContext.SaveChanges();
            return Ok("Record updated successfully.");
        }

        // DELETE: api/CommittedPlayers/5
        public IHttpActionResult Delete(int id)
        {
            var committedPlayer = recruitsDbContext.CommittedPlayers.Find(id);

            if (committedPlayer == null)
            {
                return BadRequest("No record found against this ID.");
            }

            recruitsDbContext.CommittedPlayers.Remove(committedPlayer);
            recruitsDbContext.SaveChanges();
            return Ok("Athlete has been removed from committed player list.");
        }
    }
}