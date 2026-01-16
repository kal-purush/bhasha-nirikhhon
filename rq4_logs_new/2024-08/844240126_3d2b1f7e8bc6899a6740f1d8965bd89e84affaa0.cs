using BoreholeData;
using BoreholeData.Entities;
using BoreholeData.Repositories;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace BoreholeAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BoreholeController : ControllerBase
    {
        BoreholeRepository repository;

        public BoreholeController(BoreholeContext context) 
        {
            repository = new BoreholeRepository(context);
        }

        [HttpPost]
        public int Add(Borehole bh)
        {
            return repository.Add(bh);
        }

        [HttpPost]
        public void Update(Borehole bh)
        {
            repository.Edit(bh);
        }

        [HttpDelete]
        public void Delete(int id)
        {
            repository.Delete(id);
        }

        [HttpGet]
        [Route("Get/{id}")]
        public Borehole Get(int id)
        {
            return repository.GetById(id);
        }

        [HttpGet]
        [Route("GetByArea/{x1}/{y1}/{x2}/{y2}")]
        public List<Borehole> GetByArea(int x1, int y1, int x2, int y2)
        {
            return repository.GetByArea(x1, y1, x2, y2);
        }

    }
}