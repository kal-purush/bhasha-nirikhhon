(function (GRA) {

    GRA.component = GRA.component || {};

    /**
     *
     * @param popupTitle
     * @param popupId
     * @constructor
     */
    GRA.component.FileDownloader = function FileDownloader(popupTitle, popupId) {
        var id = popupId || 'gra-file-downloader',
            title = popupTitle || "Télécharger des fichiers",
            files = [],
            render = function render(files) {
                var index,
                    total = files.length,
                    file,
                    finder = new GRA.dom.Finder(),
                    html = [
                    '<table class="table table-bordered table-condensed table-hover gra-table" style="margin: 0">',
                    '<thead>',
                    '<tr>',
                    '<th>Nom</th><th>Type</th><th>Date</th><th>Taille</th><th>Action</th>',
                    '<tr>',
                    '</thead>',
                    '<tbody>'
                ].join('');

                for (index = 0; index < total; index += 1) {
                    file = files[index];

                    html += [
                        '<tr>',
                        '<td>' + file.filename + '</td>',
                        '<td>' + file.type + '</td>',
                        '<td>' + file.date.toLocaleString() + '</td>',
                        '<td>' + file.size + '</td>',
                        '<td><a href="' + file.link + '" type="button" class="btn btn-sm btn-success"><i class="fa fa-download"></i> Télécharger</td>',
                        '</tr>'
                    ].join('');
                }

                finder.findById(id);
                finder.findByClass('modal-body');
                finder.append(html);
            };

        /**
         *
         * @param file
         */
        this.addFile = function addFile(file) {
            var utils = GRA.utils,
                fileObject = {
                    link: '#',
                    filename: utils.TokenUtils.generate(5, 10),
                    type: '',
                    date: new Date(),
                    size: 0
                };

            utils.ObjectUtils.extend(fileObject, file);
            files.push(fileObject);
        };

        /**
         *
         * @param builder
         */
        this.create = function create(builder) {
            var html,
                finder = new GRA.dom.Finder();

            builder(this);

            html = [
                '<div class="modal" tabindex="-1" role="dialog" id="' + id + '">',
                '<div class="modal-dialog modal-lg">',
                '<div class="modal-content">',
                '<div class="modal-header">',
                '<button type="button" class="close" data-dismiss="modal" aria-label="Fermer">',
                '<span aria-hidden="true">&times;</span>',
                '</button>',
                '<h4 class="modal-title"><i class="fa fa-cloud-download"></i> ' + title + '</h4>',
                '</div>',
                '<div class="modal-body" style="padding: 0">',
                '</div>',
                '<div class="modal-footer">',
                '<button type="button" class="btn btn-danger" data-dismiss="modal"><i class="fa fa-remove"></i> Réduire</button>',
                '</div>',
                '</div>',
                '</div>',
                '</div>'
            ].join('');

            finder.searchFromBody();
            finder.append(html);
            render(files);
        };

        /**
         *
         * @param fileList
         */
        this.setFiles = function setFiles(fileList) {
            files = fileList;
        };

        /**
         *
         * @param renderer
         */
        this.setRenderer = function setRenderer(renderer) {
            render = renderer;
        };
    };

}(GRA || {}));