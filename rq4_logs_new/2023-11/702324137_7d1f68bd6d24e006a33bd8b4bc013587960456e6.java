package pie.tomato.tomatomarket.presentation.member.response;

import static lombok.AccessLevel.*;

import lombok.AllArgsConstructor;
import lombok.Getter;
import pie.tomato.tomatomarket.domain.MemberTown;

@Getter
@AllArgsConstructor(access = PRIVATE)
public class MemberRegionResponse {

	private Long addressId;
	private String fullAddressName;
	private String addressName;
	private boolean isSelected;

	public static MemberRegionResponse from(MemberTown memberTown) {
		return new MemberRegionResponse(
			memberTown.getRegion().getId(),
			memberTown.getRegion().getFullAddressName(),
			memberTown.getRegion().getAddressName(),
			memberTown.isSelected()
		);
	}
}