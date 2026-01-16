package pie.tomato.tomatomarket.presentation.sales;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import pie.tomato.tomatomarket.application.item.ItemService;
import pie.tomato.tomatomarket.presentation.dto.CustomSlice;
import pie.tomato.tomatomarket.presentation.item.response.SalesItemDetailResponse;
import pie.tomato.tomatomarket.presentation.support.AuthPrincipal;
import pie.tomato.tomatomarket.presentation.support.Principal;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/sales")
public class SalesItemController {

	private final ItemService itemService;

	@GetMapping("/history")
	public ResponseEntity<CustomSlice<SalesItemDetailResponse>> salesItemDetails(
		@AuthPrincipal Principal principal,
		@RequestParam(required = false) String status,
		@RequestParam(required = false, defaultValue = "10") int size,
		@RequestParam(required = false) Long cursor) {
		return ResponseEntity.ok().body(itemService.salesItemDetails(status, principal, size, cursor));
	}
}