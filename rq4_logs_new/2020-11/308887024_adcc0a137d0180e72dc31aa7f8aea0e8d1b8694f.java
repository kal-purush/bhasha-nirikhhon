package com.marketplace.controller;

import com.marketplace.domain.*;

import java.util.Optional;

public class ClassifiedAdCommandHandler {
    private final ClassifiedAdRepository classifiedAdRepository;

    public ClassifiedAdCommandHandler(ClassifiedAdRepository classifiedAdRepository) {
        this.classifiedAdRepository = classifiedAdRepository;
    }

    public AddPictureResponse handleAddPicture(AddPictureDto addPictureDto) {
        Optional<ClassifiedAd> load = classifiedAdRepository.load(new ClassifiedAdId(addPictureDto.getClassifiedAdId()));

        return load.map(classifiedAd -> {

            PictureSize pictureSize = new PictureSize(addPictureDto.getWidth(), addPictureDto.getHeight());
            var pictureId = classifiedAd.addPicture(addPictureDto.getUri(), pictureSize);

            var savedClassifiedAd = classifiedAdRepository.save(classifiedAd);

            return new SavedResponse(savedClassifiedAd, pictureId);
        }).map(savedResponse -> {
            var response = new AddPictureResponse();
            response.id = savedResponse.pictureId.id();
            response.classifiedAdId = savedResponse.classifiedAd.getId().id();
            return response;
        }).orElse(new AddPictureResponse());
    }

    public ResizePictureResponse handleResizePicture(ResizePictureDto pictureDto) {
        Optional<ClassifiedAd> load = classifiedAdRepository.load(new ClassifiedAdId(pictureDto.getClassifiedAdId()));

        return load.map(classifiedAd -> {

            PictureSize pictureSize = new PictureSize(pictureDto.getWidth(), pictureDto.getHeight());
            var pictureId = classifiedAd.resizePicture(new PictureId(pictureDto.getId()), pictureSize);

            var savedClassifiedAd = classifiedAdRepository.save(classifiedAd);

            return new SavedResponse(savedClassifiedAd, pictureId);
        }).map(savedResponse -> {
            var response = new ResizePictureResponse();
            response.id = savedResponse.pictureId.id();
            response.classifiedAdId = savedResponse.classifiedAd.getId().id();
            return response;
        }).orElse(new ResizePictureResponse());
    }

    record SavedResponse(ClassifiedAd classifiedAd, PictureId pictureId) {
    }
}