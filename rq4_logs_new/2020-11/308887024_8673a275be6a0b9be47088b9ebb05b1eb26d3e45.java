package com.marketplace.domain;

import com.marketplace.controller.*;
import com.marketplace.domain.command.CreateClassifiedAd;
import com.marketplace.domain.command.CreateClassifiedAdCommandHandler;
import com.marketplace.domain.command.UpdateClassifiedAd;
import com.marketplace.domain.command.UpdateClassifiedAdCommandHandler;

import java.util.Optional;

public class ClassifiedAdService {
    private final ClassifiedAdRepository classifiedAdRepository;
    private final CreateClassifiedAdCommandHandler createCommandHandler;
    private final UpdateClassifiedAdCommandHandler updateCommandHandler;

    public ClassifiedAdService(ClassifiedAdRepository classifiedAdRepository,
                               CreateClassifiedAdCommandHandler createCommandHandler,
                               UpdateClassifiedAdCommandHandler updateCommandHandler) {
        this.classifiedAdRepository = classifiedAdRepository;
        this.createCommandHandler = createCommandHandler;
        this.updateCommandHandler = updateCommandHandler;
    }

    public CommandHandlerResult<CreateAdResponse> handleCreate(CreateClassifiedAd createClassifiedAd) {
        return createCommandHandler.handle(createClassifiedAd);
    }

    public CommandHandlerResult<UpdateClassifiedAdResponse> handleUpdate(UpdateClassifiedAd updateClassifiedAd) {
        return updateCommandHandler.handle(updateClassifiedAd);
    }

    public AddPictureResponse handleAddPicture(AddPictureDto addPictureDto) {
        Optional<ClassifiedAd> load = classifiedAdRepository.load(new ClassifiedAdId(addPictureDto.getClassifiedAdId()));

        return load.map(classifiedAd -> {

            PictureSize pictureSize = new PictureSize(addPictureDto.getWidth(), addPictureDto.getHeight());
            var pictureId = classifiedAd.addPicture(addPictureDto.getUri(), pictureSize);

            var savedClassifiedAd = classifiedAdRepository.add(classifiedAd);

            return new SavedResponse(savedClassifiedAd, pictureId);
        }).map(savedResponse -> {
            var response = new AddPictureResponse();
            response.setId(savedResponse.pictureId.id());
            response.setClassifiedAdId(savedResponse.classifiedAd.getId().id());
            return response;
        }).orElse(new AddPictureResponse());
    }

    public ResizePictureResponse handleResizePicture(ResizePictureDto pictureDto) {
        Optional<ClassifiedAd> load = classifiedAdRepository.load(new ClassifiedAdId(pictureDto.getClassifiedAdId()));

        return load.map(classifiedAd -> {

            PictureSize pictureSize = new PictureSize(pictureDto.getWidth(), pictureDto.getHeight());
            var pictureId = classifiedAd.resizePicture(new PictureId(pictureDto.getId()), pictureSize);

            var savedClassifiedAd = classifiedAdRepository.add(classifiedAd);

            return new SavedResponse(savedClassifiedAd, pictureId);
        }).map(savedResponse -> {
            var response = new ResizePictureResponse();
            response.setId(savedResponse.pictureId.id());
            response.setClassifiedAdId(savedResponse.classifiedAd.getId().id());
            return response;
        }).orElse(new ResizePictureResponse());
    }

    record SavedResponse(ClassifiedAd classifiedAd, PictureId pictureId) {
    }
}