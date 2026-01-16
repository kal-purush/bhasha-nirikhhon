
  export class ImageCaptionController {

    private images: any;
    private $modalInstance: bs.IModalServiceInstance;
    private imageCaptions: ImageCaption[];
    private config: ng.IHttpPromiseCallbackArg<ConfigResponse>
    private stbImage: service.images.ImageService

    constructor($modalInstance: bs.IModalServiceInstance,
                stbImage: service.images.ImageService,
                images: any,
                configPromise: ng.IHttpPromiseCallbackArg<st.response.configuration.ConfigResponse>) {
      this.images = images;
      this.$modalInstance = $modalInstance;
      this.config = configPromise.data;
      this.stbImage = stbImage;
      this.imageCaptions = _.map(this.images, function(image: ImageIdPrefix) {
        return { image: image, caption: "" }
      });

    }　

    getThumbnailImageUrl(imageId: string, prefix: string) {
      return this.stbImage.getThumbnailImageUrl(this.config, imageId, prefix);
    }

    ok(): void {
      this.$modalInstance.close(this.imageCaptions);
    }

    cancel(): void {
      this.$modalInstance.dismiss();
    }

  }