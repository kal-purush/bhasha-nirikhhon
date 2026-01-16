using EtudeManyToMany.API.Repository;
using EtudeManyToMany.Core.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace EtudeManyToMany.API.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class CommentaireController : ControllerBase
    {
        private readonly IRepository<Commentaire> _commentaireRepository;
        private readonly IRepository<Passager> _passagerRepository;
        private readonly IRepository<Conducteur> _conducteurRepository;

        public CommentaireController(IRepository<Commentaire> commentaireRepository, IRepository<Passager> passagerRepository, IRepository<Conducteur> conducteurRepository)
        {
            _commentaireRepository = commentaireRepository;
            _passagerRepository = passagerRepository;
            _conducteurRepository = conducteurRepository;
        }

        [HttpGet]
        public async Task<IActionResult> TouteLesCommentaires()
        {
            var reservations = await _commentaireRepository.GetAll();

            return Ok(reservations);
        }


        [HttpGet("{commentaireId}")]
        public async Task<IActionResult> Get(int commentaireId)
        {
            var reservations = await _commentaireRepository.GetById(commentaireId);

            return Ok(reservations);
        }
    }
}