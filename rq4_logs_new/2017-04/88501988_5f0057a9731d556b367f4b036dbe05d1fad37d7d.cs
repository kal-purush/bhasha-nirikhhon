using madHealth.API.Models;
using madHealth.Database;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace madHealth.API.Controllers
{
    [RoutePrefix("api/analysisresults")]
    public class AnalysisResultsController : BaseController
    {
        [Route("")]
        public IHttpActionResult Get()
        {
            try
            {
                return Ok(UnitOfWork.AnalysisResults.Get().ToList().Select(x => Factory.Create(x)).ToList());
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        [Route("{title}")]
        public IHttpActionResult Get(string title)
        {
            return Ok(UnitOfWork.AnalysisResults.Get().Where(x => x.Title.Contains(title)).ToList().Select(x => Factory.Create(x)).ToList());
        }


        [Route("{id:int}")]
        public IHttpActionResult Get(int id)
        {
            try
            {
                AnalysisResult type = UnitOfWork.AnalysisResults.Get(id); 
                if (type == null)
                {
                    return NotFound();
                }
                else
                {
                    return Ok(Factory.Create(type));
                }
            }
            catch (Exception ex)
            {

                return BadRequest(ex.Message);
            }
        }

        [Route("")]
        public IHttpActionResult Post(AnalysisResultModel model)
        {
            try
            {
                AnalysisResult result = Factory.Create(model);
                UnitOfWork.AnalysisResults.Insert(result);
                UnitOfWork.Commit();
                return Ok(Factory.Create(result));
            }
            catch (Exception ex)
            {

                return BadRequest(ex.Message);
            }
        }

        [Route("{id}")]
        public IHttpActionResult Put(int id, AnalysisResultModel model)
        {
            try
            {
                AnalysisResult result = Factory.Create(model);
                UnitOfWork.AnalysisResults.Update(result, id);
                UnitOfWork.Commit();
                return Ok(Factory.Create(result));
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        [Route("{id}")]
        public IHttpActionResult Delete(int id)
        {
            try
            {
                AnalysisResult entity = UnitOfWork.AnalysisResults.Get(id);
                if (entity == null) return NotFound();
                UnitOfWork.AnalysisResults.Delete(id);
                UnitOfWork.Commit();
                return Ok();
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }
    }
}