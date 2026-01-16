package pie.tomato.tomatomarket.application.memberTown;

import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import pie.tomato.tomatomarket.domain.Member;
import pie.tomato.tomatomarket.domain.MemberTown;
import pie.tomato.tomatomarket.domain.Region;
import pie.tomato.tomatomarket.exception.BadRequestException;
import pie.tomato.tomatomarket.exception.ErrorCode;
import pie.tomato.tomatomarket.exception.NotFoundException;
import pie.tomato.tomatomarket.infrastructure.persistence.member.MemberRepository;
import pie.tomato.tomatomarket.infrastructure.persistence.memberTown.MemberTownRepository;
import pie.tomato.tomatomarket.infrastructure.persistence.region.RegionRepository;
import pie.tomato.tomatomarket.presentation.memberTown.request.AddMemberTownRequest;
import pie.tomato.tomatomarket.presentation.support.Principal;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class MemberTownService {

	public static final int MEMBER_TOWN_MAXIMUM_SIZE = 1;

	private final MemberTownRepository memberTownRepository;
	private final RegionRepository regionRepository;
	private final MemberRepository memberRepository;

	@Transactional
	public void addMemberTown(Principal principal, AddMemberTownRequest memberTownRequest) {
		Member member = getMember(principal);
		Region region = regionRepository.findById(memberTownRequest.getAddressId())
			.orElseThrow(() -> new NotFoundException(ErrorCode.NOT_FOUND_REGION));

		validateMemberTownSizeAndDuplicateRegion(principal, memberTownRequest);

		memberTownRepository.save(MemberTown.of(region, member));
	}

	private void validateMemberTownSizeAndDuplicateRegion(
		Principal principal, AddMemberTownRequest memberTownRequest) {
		List<MemberTown> memberTowns = memberTownRepository.findAllByMemberId(principal.getMemberId());
		Optional<MemberTown> duplicated = memberTowns.stream()
			.filter(memberTown -> memberTown.isSameRegionId(memberTownRequest.getAddressId()))
			.findAny();

		if (duplicated.isPresent()) {
			throw new BadRequestException(ErrorCode.ALREADY_ADDRESS);
		}

		if (memberTowns.size() > MEMBER_TOWN_MAXIMUM_SIZE) {
			throw new BadRequestException(ErrorCode.MAXIMUM_MEMBER_TOWN_SIZE);
		}
	}

	private Member getMember(Principal principal) {
		return memberRepository.findById(principal.getMemberId())
			.orElseThrow(() -> new NotFoundException(ErrorCode.NOT_FOUND_MEMBER));
	}
}