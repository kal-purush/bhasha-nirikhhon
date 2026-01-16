package com.numo.api.global.comm.gtts;

import com.numo.api.global.comm.exception.CustomException;
import com.numo.api.global.comm.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.List;

@Service
@RequiredArgsConstructor
public class GttsService {

    @Value("${cstm.gtts-api}")
    private String gttsAPIServerUrl;

    public void saveAudio(Gtts request) {
        connectClient("/gtts", request);
    }

    public void saveAudio(List<Gtts> request) {
        connectClient("/gtts/list", request);
    }

    private void connectClient(String uri, Object request) {
        RestClient restClient = RestClient.create();

        ResponseEntity<Void> response = restClient.post()
                .uri(gttsAPIServerUrl + uri)
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .onStatus(status -> status.value() != 201, (req, res) -> {
                    throw new CustomException(ErrorCode.SOUND_CANNOT_CREATED);
                })
                .toBodilessEntity();
    }

}