using DnD_NPC_Generator.Models;
using DnD_NPC_Generator.WebAPI;
using Microsoft.AspNetCore.Mvc;

namespace DnD_NPC_Generator.Controllers
{
    public class SpellController : Controller
    {
        private readonly DNDConsumer spellApi;

        public SpellController(DNDConsumer consumer)
        {
            this.spellApi = consumer;
        }

        public async Task<IActionResult> Index()
        {
            SpellListView spellList = new SpellListView();
            spellList.Spells = await spellApi.GetAllSpells();

            return View(spellList);
        }
    }
}