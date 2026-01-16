package kr.ac.kumoh.illdang100.tovalley.service.lost_found_board;

import kr.ac.kumoh.illdang100.tovalley.domain.comment.CommentRepository;
import kr.ac.kumoh.illdang100.tovalley.domain.lost_found_board.LostFoundBoard;
import kr.ac.kumoh.illdang100.tovalley.domain.lost_found_board.LostFoundBoardImageRepository;
import kr.ac.kumoh.illdang100.tovalley.domain.lost_found_board.LostFoundBoardRepository;
import kr.ac.kumoh.illdang100.tovalley.domain.lost_found_board.LostFoundEnum;
import kr.ac.kumoh.illdang100.tovalley.domain.member.Member;
import kr.ac.kumoh.illdang100.tovalley.domain.member.MemberEnum;
import kr.ac.kumoh.illdang100.tovalley.domain.water_place.WaterPlace;
import kr.ac.kumoh.illdang100.tovalley.domain.water_place.WaterPlaceRepository;
import kr.ac.kumoh.illdang100.tovalley.dummy.DummyObject;
import kr.ac.kumoh.illdang100.tovalley.security.auth.PrincipalDetails;
import kr.ac.kumoh.illdang100.tovalley.security.jwt.JwtProcess;
import kr.ac.kumoh.illdang100.tovalley.service.page.PageServiceImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

import java.util.Optional;

import static kr.ac.kumoh.illdang100.tovalley.dto.lost_found_board.LostFoundBoardReqDto.*;
import static kr.ac.kumoh.illdang100.tovalley.dto.lost_found_board.LostFoundBoardRespDto.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@ActiveProfiles("test")
@ExtendWith(MockitoExtension.class)
class LostFoundBoardServiceImplTest extends DummyObject {
    @InjectMocks
    private LostFoundBoardServiceImpl lostFoundBoardService;
    @InjectMocks
    private PageServiceImpl pageService;
    @Mock
    private LostFoundBoardRepository lostFoundBoardRepository;
    @Mock
    private WaterPlaceRepository waterPlaceRepository;
    @Mock
    private CommentRepository commentRepository;
    @Mock
    private LostFoundBoardImageRepository lostFoundBoardImageRepository;
    @Mock
    private JwtProcess jwtProcess;

    @Test
    @DisplayName(value = "분실물 게시글 수정")
    public void updateLostFoundBoardTest() {
        // given
        Long memberId = 1L;
        Long waterPlaceId = 1L;
        Long lostFoundBoardId = 1L;

        LostFoundBoardUpdateReqDto lostFoundBoardUpdateReqDto = new LostFoundBoardUpdateReqDto(lostFoundBoardId, "LOST", waterPlaceId, "잃어버렸어요ㅠㅠ", "지갑 보신 분 계신가요?");

        // stub
        Member member = newMockMember(memberId, "kakao_1234", "일당백", MemberEnum.CUSTOMER);
        WaterPlace waterPlace = newWaterPlace(waterPlaceId, "금오계곡", "경북", 3.0, 3);
        LostFoundBoard lostFoundBoard = newLostFoundBoard(lostFoundBoardId, LostFoundEnum.LOST, waterPlace, member);

        when(lostFoundBoardRepository.findById(lostFoundBoardId)).thenReturn(Optional.of(lostFoundBoard));
        when(waterPlaceRepository.findById(waterPlaceId)).thenReturn(Optional.of(waterPlace));

        // when
        lostFoundBoardService.updateLostFoundBoard(lostFoundBoardUpdateReqDto, memberId);

        // stub2
        when(lostFoundBoardRepository.findById(anyLong())).thenReturn(Optional.of(lostFoundBoard));
        when(jwtProcess.verify(any())).thenReturn(new PrincipalDetails(member));

        LostFoundBoardDetailRespDto lostFoundBoardDetail = pageService.getLostFoundBoardDetail(lostFoundBoardId, "refreshToken");

        // then
        assertEquals(lostFoundBoardUpdateReqDto.getTitle(), lostFoundBoardDetail.getTitle());
        assertEquals(lostFoundBoardUpdateReqDto.getContent(), lostFoundBoardDetail.getContent());
    }
}