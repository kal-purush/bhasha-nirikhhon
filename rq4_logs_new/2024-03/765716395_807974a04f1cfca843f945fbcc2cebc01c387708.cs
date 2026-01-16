using EtudeManyToMany.API.Repository;
using EtudeManyToMany.Core.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace EtudeManyToMany.API.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class ConducteurController : ControllerBase
    {
        private readonly IRepository<Conducteur> _conducteurRepository;
        private readonly IRepository<Trajet> _trajetRepository;
        private readonly IRepository<Utilisateur> _utilisateurRepository;

        public ConducteurController(IRepository<Conducteur> conducteurRepository, IRepository<Trajet> trajetRepository, IRepository<Utilisateur> utilisateurRepository)
        {
            _conducteurRepository = conducteurRepository;
            _trajetRepository = trajetRepository;
            _utilisateurRepository = utilisateurRepository;
        }

        [HttpGet]
        public async Task<IActionResult> ToutLesConducteurs()
        {
            var conducteurs = await _conducteurRepository.GetAll();

            foreach (var conducteur in conducteurs)
            {
                conducteur.Utilisateur = await _utilisateurRepository.GetById(conducteur.UtilisateurId);

                conducteur.Trajets = await _trajetRepository.GetAll(t => t.ConducteurId == conducteur.ConducteurId);
            }

            return Ok(conducteurs);
        }



        [HttpGet("{conducteurId}")]
        public async Task<IActionResult> Get(int conducteurId)
        {
            var conducteur = await _conducteurRepository.GetById(conducteurId);

            if (conducteur == null)
            {
                return NotFound("Conducteur non trouvé");
            }

            conducteur.Utilisateur = await _utilisateurRepository.GetById(conducteur.UtilisateurId);

            conducteur.Trajets = await _trajetRepository.GetAll(t => t.ConducteurId == conducteur.ConducteurId);

            return Ok(conducteur);
        }



        [HttpPost("ajout-du-Trajet/{conducteurId}")]
        public async Task<IActionResult> AjoutTrajet(int conducteurId, [FromBody] Trajet trajet)
        {
            var conducteur = await _conducteurRepository.GetById(conducteurId);
            if (conducteur == null)
                return BadRequest("Conducteur non trouvé");

            if (conducteur.Trajets != null)
                return BadRequest("Le conducteur a déjà un trajet");

            trajet.ConducteurId = conducteurId;

            await _trajetRepository.Add(trajet);

            return Ok("Trajet ajouté avec succès et associé au Conducteur");
        }

        [HttpDelete("Retrait-du-Trajet/{conducteurId}")]
        public async Task<IActionResult> RetraitConducteur(int conducteurId, int trajetId)
        {
            if (await _conducteurRepository.GetById(conducteurId) == null)
                return BadRequest("Conducteur introuvable");

            var ing = await _trajetRepository.GetById(trajetId);

            if (ing == null)
                return BadRequest("Trajet introuvable");

            if (ing.ConducteurId != conducteurId)
                return BadRequest("Le Trajet est sur un autre conducteur");

            if (await _trajetRepository.Delete(ing.TrajetId))
                return Ok("Trajet retirer");

            return BadRequest("Oh oh ... des problèmes");
        }
    }
}