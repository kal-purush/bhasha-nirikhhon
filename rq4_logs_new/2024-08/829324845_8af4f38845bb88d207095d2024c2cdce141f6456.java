package pintoss.giftmall.domains.board.infra;


import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import pintoss.giftmall.common.exceptions.CustomException;
import pintoss.giftmall.common.exceptions.ErrorCode;
import pintoss.giftmall.domains.board.domain.Board;

@Component
@RequiredArgsConstructor
public class BoardRepositoryReader {

    private final BoardRepository boardRepository;

    public Board findById(Long id) {
        return boardRepository.findById(id).orElseThrow(() -> new CustomException(HttpStatus.NOT_FOUND, ErrorCode.NOT_FOUND, "게시판 id를 다시 확인해주세요."));
    }

}