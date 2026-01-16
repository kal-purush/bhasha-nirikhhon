using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Web.Http;
using System.Web.Http.Description;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using TrackableEntities;
using TrackableEntities.EF6;
using TrackableEntities.Common;
using AES.Entities.Shared.Net45.Models;

// NOTE: Primary key name and/or type may need to be set manually.

namespace AES.JobLibrarySVC.WebApi.Controllers
{
    public class QuestionAnswerController : ApiController
    {
        private readonly AESEntitiesModel _dbContext = new AESEntitiesModel();

        // GET api/QuestionAnswer
        [ResponseType(typeof(IEnumerable<QuestionAnswer>))]
        public async Task<IHttpActionResult> GetQuestionAnswers()
        {
            IEnumerable<QuestionAnswer> entities = await _dbContext.QuestionAnswers
                // TODO: Add Includes for reference and/or collection properties
                .ToListAsync();

            return Ok(entities);
        }

        // GET api/QuestionAnswer/5
        [ResponseType(typeof(QuestionAnswer))]
        public async Task<IHttpActionResult> GetQuestionAnswer(int id)
        {
            QuestionAnswer entity = await _dbContext.QuestionAnswers
                // TODO: Add Includes for reference and/or collection properties
                .SingleOrDefaultAsync(e => e.QuestionAnswerId == id);

            if (entity == null)
            {
                return NotFound();
            }

            return Ok(entity);
        }

        // POST api/QuestionAnswer
        [ResponseType(typeof(QuestionAnswer))]
        public async Task<IHttpActionResult> PostQuestionAnswer(QuestionAnswer entity)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            entity.TrackingState = TrackingState.Added;
            _dbContext.ApplyChanges(entity);


            try
            {
                await _dbContext.SaveChangesAsync();
            }
            catch (DbUpdateException)
            {
                if (_dbContext.QuestionAnswers.Any(e => e.QuestionAnswerId == entity.QuestionAnswerId))
                {
                    return Conflict();
                }
                throw;
            }

            await _dbContext.LoadRelatedEntitiesAsync(entity);
            entity.AcceptChanges();
            return CreatedAtRoute("DefaultApi", new { id = entity.QuestionAnswerId }, entity);
        }

        // PUT api/QuestionAnswer
        [ResponseType(typeof(QuestionAnswer))]
        public async Task<IHttpActionResult> PutQuestionAnswer(QuestionAnswer entity)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _dbContext.ApplyChanges(entity);

            try
            {
                await _dbContext.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!_dbContext.QuestionAnswers.Any(e => e.QuestionAnswerId == entity.QuestionAnswerId))
                {
                    return Conflict();
                }
                throw;
            }

            await _dbContext.LoadRelatedEntitiesAsync(entity);
            entity.AcceptChanges();
            return Ok(entity);
        }

        // DELETE api/QuestionAnswer/5
        public async Task<IHttpActionResult> DeleteQuestionAnswer(int id)
        {
            QuestionAnswer entity = await _dbContext.QuestionAnswers
                // TODO: Include child entities if any
                .SingleOrDefaultAsync(e => e.QuestionAnswerId == id);
            if (entity == null)
            {
                return Ok();
            }

            entity.TrackingState = TrackingState.Deleted;
            _dbContext.ApplyChanges(entity);

            try
            {
                await _dbContext.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!_dbContext.QuestionAnswers.Any(e => e.QuestionAnswerId == entity.QuestionAnswerId))
                {
                    return Conflict();
                }
                throw;
            }

            return Ok();
        }

        protected override void Dispose(bool disposing)
        {
            _dbContext.Dispose();
            base.Dispose(disposing);
        }
    }
}