declare var $;
module F5Pager {
    "use strict"
    export class Pagination {
        private _currentPage: number = 0;
        private _itemsPerPage: number;
        private _totalPages: number;
        private _numLinksDisplay: number = 2;
        private _elementWrapper: any;

        constructor(element: any, perPage: number = 5) {
            this._elementWrapper = element;
            this._itemsPerPage = perPage;
            this._totalPages = Math.ceil(element.find('.data > li').length / this._itemsPerPage);
            this.initPagination();
        }

        private initPagination(): void {
            var $this = this
                , current_link: number = 0
                , htmlNav: string = ''
                , htmlNextLink: string = '<a href="#proximo" title="Próximo" class="nextPage">Próximo <span>></span></a>'
                , htmlPrevLink: string = '<a href="#anterior" style="display:none;" title="Anterior" class="prevPage"><span><</span> Anterior</a>'
                , htmlNumLink: string = '<a href="{{num}}" class="page-num page-{{num}}" style="display:none;">{{num}}</a>';

            htmlNav = '<span class="navigation">';
            while ($this._totalPages > current_link) {
                htmlNav += htmlNumLink.replace(/{{num}}/g, (current_link + 1).toString());
                current_link++;
            }
            htmlNav += '</span>';

            $this._elementWrapper.find('.pager')
                .append(htmlPrevLink + htmlNav + htmlNextLink)
                .find('.navigation > a')
                .on('click', function (e) {
                    e.preventDefault();
                    $this._currentPage = parseInt($(this).text().trim()) - 1;
                    $this.changePage();
                })
                .slice(0, $this._numLinksDisplay + 1)
                .show()
                .first()
                .addClass('current-page');

            $this._elementWrapper.find('.data > li').filter(function (i) { return i > $this._itemsPerPage - 1; }).hide();

            $this._elementWrapper.find('a.prevPage').on('click', function (e) {
                e.preventDefault();
                $this._currentPage--;
                $this._elementWrapper.find('a.nextPage').show();

                if ($this._currentPage == 0)
                    $(this).hide();

                $this.changePage();
            });

            $this._elementWrapper.find('a.nextPage').on('click', function (e) {
                e.preventDefault();
                $this._currentPage++;
                $this._elementWrapper.find('a.prevPage').show();
                if ($this._currentPage == $this._totalPages - 1) {
                    $(this).hide();
                }
                $this.changePage();
            });
        }

        private changePage(): void {
            var $this = this;

            $this._elementWrapper.find('.navigation > a').hide().removeClass('current-page').eq($this._currentPage).addClass('current-page')
            $this._elementWrapper.find('.navigation > a').slice(($this._currentPage - 2 < 1 ? 0 : $this._currentPage - $this._numLinksDisplay), (($this._currentPage > $this._totalPages) ? $this._totalPages : $this._currentPage + $this._numLinksDisplay + 1)).show();

            $this._elementWrapper.find('.data > li').hide();
            $this._elementWrapper.find('.data > li').filter(function (i) {
                var p = ($this._currentPage * $this._itemsPerPage);
                return i >= p && i < p + $this._itemsPerPage;
            }).show();
        }
    }
}

new F5Pager.Pagination($('.list'));