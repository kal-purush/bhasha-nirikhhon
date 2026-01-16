package kr.ac.kumoh.illdang100.tovalley.service.lost_found_board;

import kr.ac.kumoh.illdang100.tovalley.domain.ImageFile;
import kr.ac.kumoh.illdang100.tovalley.domain.lost_found_board.LostFoundBoardImage;
import kr.ac.kumoh.illdang100.tovalley.domain.lost_found_board.LostFoundBoardImageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class LostFoundBoardImageServiceImpl implements LostFoundBoardImageService {
    private final LostFoundBoardImageRepository lostFoundBoardImageRepository;

    @Override
    @Transactional
    public void saveLostFoundImageFile(List<ImageFile> uploadImageFiles, Long lostFoundBoardId) {
        uploadImageFiles.stream()
                .map(imageFile -> LostFoundBoardImage.builder().lostFoundBoardId(lostFoundBoardId).imageFile(imageFile).build())
                .forEach(lostFoundBoardImageRepository::save);
    }
}