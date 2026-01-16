/// <reference path="../../services/common/routes.ts" />
/// <reference path="../../services/common/st-service.ts" />
/// <reference path="../../services/common/stb-service.ts" />
/// <reference path="../../controllers/image-popup-controller.ts" />

module stanby.directives.common {

  import ImageCaption = stanby.models.images.ImageCaption;
  import InlineImage  = stanby.models.images.InlineImage;

  export class MarkdownTextarea implements ng.IDirective {

    static $inject = ["stbImagePopup", "stbImage", "stModal"]

    constructor(
      private stbImagePopup: service.images.ImagePopupService,
      private stbImage: service.images.ImageService,
      private stModal: std.Modal,
      private routes: st.Routes
    ) {}

    restrict = 'E';
    templateUrl = '/templates/markdownTextarea.html';
    transclude = true;
    scope = {
      config: "=",
      previewMarkdown: "=",
      flagFocusedMarkdown: "=",
      model: "=ngModel",
      focusFunction: "&",
      editFunction: "&",
      previewFunction: "&"
    };

    link = (scope, element: ng.IAugmentedJQuery, attrs: ng.IAttributes) => {
      scope.showImagePopup = () => {
        // open the image popup
        this.stbImagePopup.showInlineImagePopup((results: any[]) => {
          scope.loadCaptionWizard(results);
        });
      }

      // inline upload used when drag and dropping an image to the textarea
      scope.inlineUpload = (element, params) => {
        this.routes.images.uploadInline(params.form)
          .success((res) => {
            // create a InlineImage
            var image: InlineImage = { imageId: res.id, prefix: res.prefix };

            scope.loadCaptionWizard([image])
          });
      }

      // handle the image uploaded result
      scope.loadCaptionWizard = (results: any[]) => {
        if (results && results.length > 0) {
          var
            $element = $('#jsi-markdown-freetext'),
            $textArea: any = $element[0],
            beforeText, afterText,
            text = $element.val(),
            position = $textArea.selectionStart;


          // open a dialog to allow captioning of selected image(s)
          scope.captionDialog(results, (captions) => {
            var imgListTxt = _.map(captions, (res: ImageCaption) => {
              var imageUrl = this.stbImage.getFullImageUrl(scope.config, res.image.imageId, res.image.prefix);
              var caption = (res.caption === "" ? ' ' : res.caption);
              // open a modal dialog
              return '![' + caption + '](' + imageUrl + ' "' + caption + '")\n';
            }).join('\n');

            if (!scope.flagFocusedMarkdown || position === text.length) {
              position = 0;
            }

            if (position !== 0) {
              imgListTxt = '\n' + imgListTxt;
            }

            beforeText = text.slice(0, position);
            afterText = text.slice(position, text.length);

            text = beforeText + imgListTxt + afterText;

            scope.model = text;
          });
        }
      }

      // open a modal dialog for captions
      scope.captionDialog = (images, cbOnClose: (res: ImageCaption) => void) => {
        var modalConfig = {
          templateUrl: '/templates/images/images-caption.html',
          controller: 'ImageCaptionCtrl as c',
          resolve: {
            images: () => {
              return images;
            },
            configPromise: (stbConfig: stb.ConfigService) => {
              return stbConfig.getConfigPromise();
            }
          }
        };

        this.stModal.modalCustom(modalConfig).result.then(cbOnClose);
      }

    };
  }

  export function markdownTextarea() {
    angular.module('stanbyDirectives')
    .directive('stMarkdownTextarea', (stbImagePopup: service.images.ImagePopupService,
                                      stbImage: service.images.ImageService,
                                      stModal: std.Modal,
                                      routes: st.Routes) => {
        return new MarkdownTextarea(stbImagePopup, stbImage, stModal, routes);
    })　
  }
}