package vn.hust.hedspi.ezsport.services;

import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import vn.hust.hedspi.ezsport.dtos.ApiResponse;
import vn.hust.hedspi.ezsport.dtos.sport.CreateSportRequest;
import vn.hust.hedspi.ezsport.dtos.sport.SportResponse;
import vn.hust.hedspi.ezsport.dtos.sport.UpdateSportRequest;
import vn.hust.hedspi.ezsport.entities.Sport;
import vn.hust.hedspi.ezsport.mappers.SportMapper;
import vn.hust.hedspi.ezsport.repositories.SportRepository;

@Service
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class SportService {
    SportRepository sportRepository;
    SportMapper sportMapper;

    public ApiResponse<Page<SportResponse>> listSport(Pageable pageable) {
        ApiResponse response = new ApiResponse();
        Page<Sport> sportPage = sportRepository.findAll(pageable);
        Page<SportResponse> sportResponsePage = sportPage.map(sportMapper::toSportResponse);
        response.setMessage("Get list sport successful !");
        response.setResult(sportResponsePage);

        return response;
    }

    public ApiResponse<SportResponse> createSport(CreateSportRequest request) {
        Sport sport = sportMapper.toCreateSportRequest(request);
        SportResponse sportResponse = sportMapper.toSportResponse(sportRepository.save(sport));
        ApiResponse response = new ApiResponse();
        response.setMessage("Create sport successful !");
        response.setResult(sportResponse);

        return response;
    }

    public ApiResponse<SportResponse> getSportById(String id) {
        Sport sport = sportRepository.findById(id).orElse(null);
        SportResponse sportResponse = sportMapper.toSportResponse(sport);
        ApiResponse response = new ApiResponse();
        response.setMessage("Get sport successful !");
        response.setResult(sportResponse);

        return response;
    }

    public ApiResponse<SportResponse> updateSport(String id, UpdateSportRequest request) {
        Sport sport = sportRepository.findById(id).orElse(null);
        if (sport == null) {
            ApiResponse response = new ApiResponse();
            response.setCode(404);
            response.setMessage("Sport not found !");

            return response;
        }

        sport.setName(request.getName());
        SportResponse sportResponse = sportMapper.toSportResponse(sportRepository.save(sport));
        ApiResponse response = new ApiResponse();
        response.setMessage("Update sport successful !");
        response.setResult(sportResponse);

        return response;
    }

    public ApiResponse<Void> deleteSport(String id) {
        sportRepository.deleteById(id);
        ApiResponse response = new ApiResponse();
        response.setMessage("Delete sport successful !");

        return response;
    }
}