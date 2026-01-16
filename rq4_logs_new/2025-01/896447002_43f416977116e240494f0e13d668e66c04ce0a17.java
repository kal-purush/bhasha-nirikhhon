package com.example.musing.playlist.controller;

import com.example.musing.board.dto.CreateBoardResponse;
import com.example.musing.board.entity.Board;
import com.example.musing.common.dto.ResponseDto;
import com.example.musing.playlist.entity.PlayList;
import com.example.musing.playlist.repository.PlayListRepository;
import com.example.musing.playlist.service.YoutubeService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController("/musing/playlist")
public class PlaylistController {

    private final YoutubeService youtubeService;
    private final PlayListRepository playListRepository;

    public PlaylistController(YoutubeService youtubeService, PlayListRepository playListRepository) {
        this.youtubeService = youtubeService;
        this.playListRepository = playListRepository;
    }

    @GetMapping(value ="/getMyPlaylist", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseDto<PlayList> getAllBoards() {
//        List<CreateBoardResponse> responseList = boardService.getAllBoards();
        return ResponseDto.of(null,"성공적으로 리스트를 불러왔습니다.");

    }
    @GetMapping("/getMyPlaylists")
    public ResponseDto<List<PlayList>> getMyPlaylists(@RequestHeader("Authorization") String bearerToken) {
        String accessToken = bearerToken.replace("Bearer ", ""); // Bearer 토큰에서 Access Token 추출

        // YouTube API 호출 및 재생목록 가져오기
        List<PlayList> playlists = youtubeService.getUserPlaylists(accessToken);

        // 데이터베이스에 저장
        playlists.forEach(playList -> {
            if (!playListRepository.existsByYoutubePlaylistId(playList.getYoutubePlaylistId())) {
                playListRepository.save(playList);
            }
        });

        return ResponseDto.of(playlists, "성공적으로 유저 재생목록을 불러왔습니다.");
    }
}