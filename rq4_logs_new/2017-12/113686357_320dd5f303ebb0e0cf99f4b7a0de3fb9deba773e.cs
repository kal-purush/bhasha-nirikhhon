using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Description;
using ChoreScore.Models;

namespace ChoreScore.Controllers
{
    [RoutePrefix("api/rewards")]
    public class RewardsController : ApiController
    {
        private ApplicationDbContext db = new ApplicationDbContext();

        // GET: api/Rewards
        [Route(""), HttpGet]
        public IQueryable<Reward> GetRewards()
        {
            return db.Rewards;
        }

        // GET: api/Rewards/5
        [ResponseType(typeof(Reward))]
        [Route("{id}"), HttpGet]
        public async Task<IHttpActionResult> GetReward(int id)
        {
            Reward reward = await db.Rewards.FindAsync(id);
            if (reward == null)
            {
                return NotFound();
            }

            return Ok(reward);
        }

        // PUT: api/Rewards/{id}
        [ResponseType(typeof(void))]
        [Route("{id}/redeem"), HttpPut]
        public async Task<IHttpActionResult> PutReward(int id, Reward reward)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            if (id != reward.Id)
            {
                return BadRequest();
            }

            db.Entry(reward).State = EntityState.Modified;

            try
            {
                await db.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!RewardExists(id))
                {
                    return NotFound();
                }
                else
                {
                    throw;
                }
            }

            return StatusCode(HttpStatusCode.NoContent);
        }

        // POST: api/Rewards
        [ResponseType(typeof(Reward))]
        [Route("add"), HttpPost]
        public async Task<HttpResponseMessage> PostReward(Reward reward)
        {
            if (!ModelState.IsValid)
            {
                return Request.CreateResponse(HttpStatusCode.OK, reward);
            }

            db.Rewards.Add(reward);
            await db.SaveChangesAsync();

            return Request.CreateResponse(HttpStatusCode.Created, reward);
        }

        // DELETE: api/Rewards/remove/{id}
        [ResponseType(typeof(Reward))]
        [Route("{id}"), HttpDelete]
        public async Task<IHttpActionResult> DeleteReward(int id)
        {
            Reward reward = await db.Rewards.FindAsync(id);
            if (reward == null)
            {
                return NotFound();
            }

            db.Rewards.Remove(reward);
            await db.SaveChangesAsync();

            return Ok(reward);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                db.Dispose();
            }
            base.Dispose(disposing);
        }

        private bool RewardExists(int id)
        {
            return db.Rewards.Count(e => e.Id == id) > 0;
        }
    }
}