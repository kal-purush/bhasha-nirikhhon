const axios = require('axios');

const cleanupResourcesFromLocationUpdate = (location, req) => {
    let imagesToBeDeleted = [];
    let videosToBeDeleted = [];
    if (req.body.bannerImage && location.bannerImage) {
        if (req.body.bannerImage.src !== location.bannerImage.src) {
            imagesToBeDeleted.push(location.bannerImage.src);
        }
    }
    if (location.detailPageImages && req.body.detailPageImages) {
        imagesToBeDeleted = imagesToBeDeleted.concat(
            location.detailPageImages
                .filter(
                    (image) =>
                        !req.body.detailPageImages.find(
                            (requestImage) => requestImage.src === image.src
                        )
                )
                .map((item) => item.src)
        );
    }
    if (req.body.thumbnailImage && location.thumbnailImage) {
        if (req.body.thumbnailImage.src !== location.thumbnailImage.src) {
            imagesToBeDeleted.push(location.thumbnailImage.src);
        }
    }
    if (location.detailPageVideos && req.body.detailPageVideos) {
        videosToBeDeleted = location.detailPageVideos.filter(
            (video) =>
                !req.body.detailPageVideos.find(
                    (requestVideo) => requestVideo.src === video.src
                )
        );
        imagesToBeDeleted = imagesToBeDeleted.concat(
            videosToBeDeleted
                .filter((video) => !!video.poster)
                .map((video) => video.poster)
        );
        imagesToBeDeleted = imagesToBeDeleted.concat(
            location.detailPageVideos
                .filter((video) => !!video.poster)
                .filter(
                    (video) =>
                        !req.body.detailPageVideos.find(
                            (requestVideo) =>
                                requestVideo.poster === video.poster
                        )
                )
                .map((video) => video.poster)
        );
        videosToBeDeleted = videosToBeDeleted.map((video) => video.src);
    }
    console.log('imagesToBeDeleted :>> ', imagesToBeDeleted);
    imagesToBeDeleted = imagesToBeDeleted.map(
        (url) => url.match(/TCG.[^.]*/)[0]
    );
    videosToBeDeleted = videosToBeDeleted.map(
        (url) => url.match(/GARAGEVIDEOS.[^.]*/)[0]
    );

    imagesToBeDeleted.length > 0 &&
        axios
            .delete(
                `https://${process.env.API_KEY}:${
                    process.env.API_SECRET
                }@api.cloudinary.com/v1_1/${
                    process.env.CLOUD_NAME
                }/resources/image/upload?${imagesToBeDeleted
                    .map((url) => 'public_ids[]=' + url)
                    .join('&')}`
            )
            .then((res) => {
                console.log(
                    'The Following Images Were Deleted:',
                    imagesToBeDeleted
                );
            })
            .catch((err) => {
                console.log(err);
            });

    videosToBeDeleted.length > 0 &&
        axios
            .delete(
                `https://${process.env.API_KEY}:${
                    process.env.API_SECRET
                }@api.cloudinary.com/v1_1/${
                    process.env.CLOUD_NAME
                }/resources/video/upload?${videosToBeDeleted
                    .map((url) => 'public_ids[]=' + url)
                    .join('&')}`
            )
            .then((res) => {
                console.log(
                    'The Following Videos Were Deleted:',
                    videosToBeDeleted
                );
            })
            .catch((err) => {
                console.log(err);
            });
};

module.exports = {
    cleanupResourcesFromLocationUpdate
};