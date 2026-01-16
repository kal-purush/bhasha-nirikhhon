using ForgingAhead.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;

namespace ForgingAhead.Controllers
{
	public class EquipCharController : Controller
	{
		private readonly ApplicationDbContext _context;

		public EquipCharController(ApplicationDbContext context)
		{
			_context = context;
		}

		public IActionResult Index()
		{
			ViewData["Title"] = "List of Equipments Per Character";
			var model = _context.EquipChars.OrderBy(c => c.CharacterId).ToList();
			return View(model);
		}

		public IActionResult EquipPerChar(int id)
		{
			ViewData["Title"] = "Equipment per Character";
			//var model = _context.EquipChar.Include(Character).Include(Equipment).FromSql("SELECT characters.Name, equipmentspercharacter.*, equipments.Name, equipments.Tipo	FROM equipmentspercharacter INNER JOIN characters ON characters.ID = equipmentspercharacter.CharacterId INNER JOIN equipments ON equipments.ID = equipmentspercharacter.EquipmentId WHERE (characters.ID = 3 AND equipmentspercharacter.IsEquiped =1)").ToList();
			var model = _context.Characters.Include(ec => ec.EquipChars).ThenInclude(e => e.Equipment).ToList();
			return View(model);
		}
	}
}