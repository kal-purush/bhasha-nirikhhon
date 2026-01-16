'use strict';

angular.module('app.attachments').controller('attachmentsCtrl', function ($scope, $rootScope, fileManager, APP_CONFIG, $stateParams, User) {

    /* jshint validthis:true */
    var vm = this;
    vm.title = 'File Manager';
    vm.files = fileManager.files;
    vm.uploading = false;
    vm.previewFile;
    vm.currentFile;
    vm.remove = fileManager.remove;
    vm.download = fileManager.download;
    vm.setPreviewFile = setPreviewFile;
    vm.setCurrentFile = setCurrentFile;
    vm.getWord = getWord;
    vm.readonly = false;

    $scope.showUpload = false;

    fileManager.params.schema = this.dbschema;
    if (!fileManager.params.schema)
    {
        fileManager.params.schema = $stateParams.schema;
    }
   
    fileManager.params.cls = this.dbclass;
    if (!fileManager.params.cls) {
        fileManager.params.cls = $stateParams.class;
    }

    fileManager.params.oid = this.oid;

    if (!fileManager.params.oid && !$stateParams.rclass) {
        fileManager.params.oid = $stateParams.oid;
    }

    if (fileManager.params.oid) {
        if ($stateParams.readonly && $stateParams.readonly === "true") {
            vm.readonly = true;
        }
        else if (this.read && this.read === true)
        {
            vm.readonly = true;
        }
        else {
            vm.readonly = false;
        }
    }
    else {
        vm.readonly = true;
    }

    fileManager.params.prefix = getTaskAttachmentPrefix(this.dbschema, this.dbclass, this.oid);

    fileManager.params.api = "api/blob";

    fileManager.params.serviceBase = APP_CONFIG.ebaasRootUrl;

    activate();

    function activate() {
        fileManager.load();
    }

    function setPreviewFile(file) {
        console.debug("setPreviewFile");
        vm.previewFile = file
    }

    function setCurrentFile(file) {
        console.debug("setCurrentFile");
        vm.currentFile = file
    }

    function remove(file) {
        fileManager.remove(file).then(function () {
            setPreviewFile();
        });
    }

    function getWord(key)
    {
        return $rootScope.getWord(key);
    }

    function getTaskAttachmentPrefix(schemaName, className, objId) {

        return "Attachments\\" + schemaName + " 1.0\\" + className + "\\" + objId;
    }

    $scope.uploadFile = function () {
        $scope.processDropzone(fileManager);
    }

    $scope.reset = function () {
        $scope.resetDropzone();
    }

    $scope.$on('instanceCreated', function (event, args) {
        fileManager.params.oid = args.oid;
        vm.readonly = false;
    });

    $scope.setShowUpload = function(status)
    {
        $scope.showUpload = status;
    }

    $scope.$on('relatedModalFormClosed', function (event, args) {
        if (fileManager.params.oid != args.masterOid) {
            fileManager.params.oid = args.masterOid;
            fileManager.load();
        }
    });

    $scope.dropzoneConfig = {
        'options': {
            url: APP_CONFIG.ebaasRootUrl + "/" + fileManager.params.api + "/" + encodeURIComponent(fileManager.params.schema) + "/" + fileManager.params.cls + "/" + fileManager.params.oid + "?prefix=" + encodeURIComponent(fileManager.params.prefix) + "&user=" + encodeURIComponent(User.userName),
            maxFilesize: 100,
            maxFiles: 20,
            maxThumbnailFilesize: 10,
            previewTemplate: '<div class="dz-preview dz-file-preview"><div><div class="dz-filename"><span data-dz-name></span></span></div><span class="fa fa-lg fa-file-text-o"></span></div><div><span class="dz-size" data-dz-size></div><div><span class="dz-upload" data-dz-uploadprogress></span></div><div class="dz-success-mark"><span class="fa fa-check"></span></div><div class="dz-error-mark"><span class="fa fa-exclamation-triangle"></span></div><div class="dz-error-message"><span data-dz-errormessage></span></div></div>',
            addRemoveLinks: false,
            paramName: "uploadFile",
            parallelUploads: 20,
            autoProcessQueue: false,
            dictDefaultMessage: '<span class="text-center"><span class="font-md visible-lg-block"><span class="font-md"><i class="fa fa-caret-right text-danger"></i><span class="font-xs">' + $rootScope.getWord("DropZone") + '</span></span>',
            dictResponseError: $rootScope.getWord("UploadError"),
            dictCancelUpload: "Cancel Upload",
            dictRemoveFile: "Remove File",
        },
        'eventHandlers' : {
            'addedFile': function (file) {
                scope.file = file;
                if (this.files[1] != null) {
                    this.removeFile(this.files[0]);
                }
                scope.$apply(function () {
                    scope.fileAdded = true;
                });
            },

            'success': function (file, response) {
            },

            'removedFile': function (file) {
                console.debug("Removed file called");
            },

            'queuecomplete': function () {
                fileManager.load();

                setTimeout(function () {
                    //scope.resetDropzone();
                }, 2000);
            }
        },
    };
});