package com.example.hackathon.content.application;

import com.example.hackathon.content.dto.ContentDto;
import com.example.hackathon.content.dto.InterestContentResponse;
import com.example.hackathon.content.dto.ScrapContentResponse;
import com.example.hackathon.content.dto.SimpleContentDto;
import com.example.hackathon.dataset.domain.Data;
import com.example.hackathon.dataset.domain.DataField;
import com.example.hackathon.dataset.domain.DataFieldUser;
import com.example.hackathon.dataset.repository.DataFieldRepository;
import com.example.hackathon.dataset.repository.DataFieldUserRepository;
import com.example.hackathon.dataset.repository.DataRepository;
import com.example.hackathon.exception.BadRequestException;
import com.example.hackathon.user.model.User;
import com.example.hackathon.user.repository.UserRepository;
import lombok.AllArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
@Service
public class ContentService {
    private final UserRepository userRepository;
    private final DataRepository dataRepository;
    private final DataFieldRepository dataFieldRepository;
    private final DataFieldUserRepository dataFieldUserRepository;
    public InterestContentResponse getInterestContentResponse(Long userId, Integer pagesize){
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new BadRequestException("유저 토큰 값을 다시 확인해주세요"));
        Optional<DataFieldUser> dataFieldUser = dataFieldUserRepository.findByUserId(userId);
        if(dataFieldUser.isPresent()){
            DataField dataField = dataFieldUser.get().getDataField();
            if(dataField.getGenre().equals("") || dataField.getGenre().equals("No genres found")){
                Pageable pageable = PageRequest.of(0, pagesize);
                List<DataField> dataFields = dataFieldRepository.findDataFieldRandom(pageable);
                List<ContentDto> contentDtos = getContentDto(dataFields, user);
                return new InterestContentResponse(contentDtos);
            }
            else{
                String[] parts = dataField.getGenre().split(",");
                Pageable pageable = PageRequest.of(0, pagesize);
                if (parts.length > 0) {
                    String firstPart = parts[0];
                    List<DataField> dataFields = dataFieldRepository.findDataFieldByGenre(firstPart, pageable);
                    List<ContentDto> contentDtos = getContentDto(dataFields, user);
                    return new InterestContentResponse(contentDtos);
                } else {
                    List<DataField> dataFields = dataFieldRepository.findDataFieldByGenre(dataField.getGenre(), pageable);
                    List<ContentDto> contentDtos = getContentDto(dataFields, user);
                    return new InterestContentResponse(contentDtos);
                }

            }
        }
        else{
            Pageable pageable = PageRequest.of(0, pagesize);
            List<DataField> dataFields = dataFieldRepository.findDataFieldRandom(pageable);
            List<ContentDto> contentDtos = getContentDto(dataFields, user);
            return new InterestContentResponse(contentDtos);
        }
    }

    public ScrapContentResponse getScrapContentResponse(Long userId){
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new BadRequestException("유저 토큰 값을 다시 확인해주세요"));
        List<DataFieldUser> dataFieldUser = dataFieldUserRepository.findListByUserId(userId);
        if(dataFieldUser==null){
            return new ScrapContentResponse(null);
        }
        else{
            List<SimpleContentDto> simpleContentDtos = getSimpleContentDto(dataFieldUser, user);
            return new ScrapContentResponse(simpleContentDtos);
        }
    }

    public List<SimpleContentDto> getSimpleContentDto(List<DataFieldUser> dataFieldUsers, User user){
        List<SimpleContentDto> simpleContentDtos = new ArrayList<>();
        for(DataFieldUser dataFieldUser : dataFieldUsers){
            DataField dataField = dataFieldUser.getDataField();
            SimpleContentDto simpleContentDto = SimpleContentDto.builder()
                    .contentId(dataField.getId())
                    .contentTitle(dataField.getTitleName())
                    .contentImage(dataField.getImage())
                    .isScraped(isScraped(user, dataField))
                    .build();
            simpleContentDtos.add(simpleContentDto);
        }
        return simpleContentDtos;
    }

    public List<ContentDto> getContentDto(List<DataField> dataFields, User user){
        List<ContentDto> contentDtos = new ArrayList<>();
        for(DataField dataField : dataFields){
            Data data = dataRepository.findByTitle(dataField.getTitleName())
                    .orElseThrow(() -> new BadRequestException("작품 정보를 다시 확인해주세요"));
            String[] parts = data.getAddress().split(" ");
            String result = parts[0] + " " + parts[1];
            ContentDto contentDto = ContentDto.builder()
                    .contentId(dataField.getId())
                    .contentTitle(dataField.getTitleName())
                    .contentImage(dataField.getImage())
                    .isScraped(isScraped(user, dataField))
                    .address(result)
                    .build();
            contentDtos.add(contentDto);
        }
        return contentDtos;
    }
    public Boolean isScraped(User user, DataField dataField){
        Optional<DataFieldUser> dataFieldUser = dataFieldUserRepository.findByUserAndDataField(user, dataField);
        if (dataFieldUser.isEmpty())
            return false;
        else
            return true;
    }
}