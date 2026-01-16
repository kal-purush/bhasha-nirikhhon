package sysjm3.bulbo.Bulbo.Controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import org.springframework.web.bind.annotation.RestController;

import sysjm3.bulbo.Bulbo.Repo.CardsRepo;
import sysjm3.bulbo.Bulbo.model.Card;

@RestController
public class CardsController {

	@Autowired
	CardsRepo cardsRepo;

	@GetMapping("/addcard")
	public String addCard(Model model) {
		// TODO
		// Add card to List<Card>
		cardsRepo.save(new Card("testCard"));
		return "Save card";
	}

	@GetMapping("/removecard")
	public String removeCard(Model model) {
		// TODO
		// Remove a card from List<Card>
		cardsRepo.delete(null);
		return "Card removed";
	}

		@GetMapping("/updatecard")
		public String updateCard(Model model) {
		// TODO
		// Update a card from List<Card>{
		return null;
	}

}