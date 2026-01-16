/**
 *@SuperGrid
 * @version 0.1
 * @namespace SuperGrid
 * @description A feature rich, developer friendly grid for client side javascript.
 * @param {Boolean} paginate Turn paging on
 */
$.widget('custom.SuperGrid', {
    /**
     * @namespace SuperGrid_Options
     * @description Options for the SuperGrid widget
     * @type {Object}
     */
    options: {
        /**
         * @name SuperGrid_Options#autoHeight
         * @description Allows SuperGrid to compute it's own height based on the parent container's height.
         * The parent container must have a height or weird things will happen. The body's height will be calculated by
         * taking the container height and subtracting the (relatively fixed) height of the header and footer.
         * Optionally you can provide an arbitrary amount of height to remove from grid body in addition to the standard math.
         * This can be useful to allow you to scale the grid to changes in the screen size.
         * @type {Object}
         * @property {object}  autoHeight               - The autoHeight option object.
         * @property {boolean}  autoHeight.enabled       - Turns on/off auto height calculation.
         * @property {number}  autoHeight.removeHeight         - A number in pixels to remove from the body in addition to the header and footer height.
         * @defaultvalue False
         */
        autoHeight: {
            enabled: false,
            removeHeight: 0
        },

        /**
         * @name SuperGrid_Options#colReorder
         * @description Defines if column reordering should be enabled.
         * If enabled this will allow the user to reorder columns by dragging the columns header to a new place.
         * @type {boolean}
         * @defaultvalue False
         */
        colReorder: false,
        /**
         * @name SuperGrid_Options#paginate
         * @description Defines if pagination should be enabled. If enabled this will provide a basic paging footer
         * Note: This options will be automatically disabled if accessibility is enabled.
         * @type {boolean}
         * @defaultvalue True
         */
        paginate: true,
        /**
         * @name SuperGrid_Options#accessibility
         * @description Defines if accessibility mode should be enabled.
         * Note: This options will automatically several options if enabled
         * @type {boolean}
         * @defaultvalue True
         */
        accessibility: false,
        /**
         * @name SuperGrid_Options#pageSize
         * @description Defines how many rows should be on a page. Must be at least 1 page,
         * if the page size is larger than the number of rows of data then only 1 page will be shown.
         * @type {Number}
         * @defaultvalue 25
         */
        pageSize: 25,
        /**
         * @name SuperGrid_Options#_grid
         * @description Array that is used internally to build the grid.
         * @type {Array}
         * @private
         * @defaultvalue empty
         */
        _grid: [],
        /**
         * @name SuperGrid_Options#_header
         * @description Array that is used to build the header internally.
         * @type {Array}
         * @private
         * @defaultvalue 25
         */
        _header: []
    },

    /**
     * @name SuperGrid#create
     * @function
     * @constructor
     * @description constructor, only called once. Starts the life cycle of the gadget.
     * This calls {@link SuperGrid#_renderGrid} and {@link SuperGrid#_bindListeners}
     * @private
     */
    _create: function () {
        // default column width
        $.each(this.options.columns, function (index, column) {
            if (!column.width) {
                column.width = 25;
            }
        });

        //Private options defaults
        this.options._grid = [];
        this.options.pagination = {
            currentPage: 1,
            numberOfPages: 1,
            pageSize: (this.options.pageSize > 0) ? this.options.pageSize : 1
        };

        //Bootstrap the UI
        this._renderGrid();
        this._bindListeners();
    },

    /**
     * @name SuperGrid#_bindListeners
     * @function
     * @description Binds the widgets events to the dom after the dom has been rendered.
     * @private
     */
    _bindListeners: function () {
        var context = this,
            resizing = false,
            resizer;

        this.element.find('.supergrid_body').off('scroll');
        this.element.find('.supergrid_body').on('scroll', function (event) {
            context.element.find('.supergrid_header').scrollLeft($(event.currentTarget).scrollLeft());
        });
        this.element.off('click', '.supergrid_header .supergrid_cell[data-sortable="true"]');
        this.element.on('click', '.supergrid_header .supergrid_cell[data-sortable="true"]', function (e) {
            var $elem = $(this),
                currSort = $elem.attr('data-sort'),
                id = $elem.attr('data-id'),
                columns = context.options.columns,
                newSort;

            context.element.find('.supergrid_header .supergrid_row').removeAttr('data-sort');

            switch (currSort) {
                case 'asc':
                    newSort = 'desc';
                    break;
                case 'desc':
                    newSort = 'asc';
                    break;
                default:
                    newSort = 'asc';
            }
            $.each(columns, function (i, col) {
                if (col.id === id) {
                    col.sort = newSort;
                    return true;
                }
                delete col.sort;
            });

            $elem.attr('data-sort', newSort);
            context._trigger('-sorted', e, {
                columns: context.options.columns
            });
            context._renderGrid();
        });

        this.element.off('mousedown', '.supergrids_header .resize-handle');
        this.element.on('mousedown', '.supergrid_header .resize-handle', function (e) {
            e.preventDefault();

            resizer = this;
            resizing = true;

            context.element.find('.supergrid_header .supergrid_cell').css('transition', 'linear');
            context.element.find('.supergrid_header').addClass('resizing');

            $(document).mousemove(function (e) {
                $(resizer).css("left", e.pageX + 2);
                context.element.find('.supergrid_header .supergrid_cell[data-id="' + $(resizer).data('id') + '"]')
                    .css("width", e.pageX - $(resizer).data('diff'));
            });

        });
        this.element.off('click', '.supergrid_footer button.left');
        this.element.on('click', '.supergrid_footer button.left', function () {
            var button = $(this);
            if (button.attr('data-state') === 'disabled') {
                return;
            }
            //Dec currentPage
            context.options.pagination.currentPage -= 1;
            //render the grid
            context._renderGrid();

        });
        this.element.off('click', '.supergrid_footer button.right');
        this.element.on('click', '.supergrid_footer button.right', function () {
            var button = $(this);
            if (button.attr('data-state') === 'disabled') {
                return;
            }
            //Inc currentPage
            context.options.pagination.currentPage += 1;
            context._renderGrid();
        });
        $(document).mouseup(function (e) {
            var colWidth, colId;
            if (resizing) {
                $(document).unbind('mousemove');
                colWidth = e.pageX - $(resizer).data('diff');
                colId = $(resizer).data('id');
                context.element.find('.supergrid_body .supergrid_cell[data-id="' + colId + '"]').css("width", colWidth);
                context.element.find('.supergrid_header .supergrid_cell').css('transition', '.2s ease-in');
                context._updateHeader(colId, colWidth);
                context.element.find('.supergrid_header').removeClass('resizing');

                $(resizer).css('left', 'auto');
                resizing = false;
            }
        });
    },

    /**
     * @name SuperGrid#_renderGrid
     * @description Render data and columns
     * @private
     * @function
     * {@link SuperGrid#_sortData}
     * {@link SuperGrid#_buildGrid}
     * {@link SuperGrid#_addMetaData}
     * @fires SuperGrid#supergrid-rendered
     */
    _renderGrid: function () {
        //Pre-sort & Pre-page
        this._sortData();
        this._pagination();

        //Build the grid
        this._buildGrid();
        this.element.html(this.options._grid.join(''));
        this.options._grid = [];

        //Finally Render Met and tell the world we rendered the grid:
        this._updatePages(this.options.pagination.currentPage);
        this._addMetaData();
        if (this.options.colReorder) {

            this.element.find('.supergrid_header').sortable({
                axis: 'x',
                containment: 'parent',
                tolerance: 'pointer',
                start: function (event, ui) {
                    ui.item.startPos = ui.item.index();

                    //setTimeout(()=>{debugger;},1000)
                },
                stop: function () {
                    // var startPosition = ui.item.startPos;
                    // var currentPosition = ui.item.index();
                    var config = this.options.columns,
                        id = 0,
                        newConfig = [],
                        existingConfigObj = {};
                    $.each($('.supergrid_header .supergrid_cell'), function (idx, item) {
                        id = $(item).data('id');
                        existingConfigObj = config.filter(function (value) {
                                return value.id.toString() === id.toString();
                            })[0] || {
                                id: '',
                                width: 25
                            };
                        newConfig.push(existingConfigObj);
                    });
                    //update the config and re-render
                    this.updateGrid(null, newConfig);
                }.bind(this)
            }).disableSelection();

        }
        //run height calc
        if (this.options.autoHeight.enabled) {
            this._renderAutoHeight();
        }
        this._trigger('supergrid-rendered');
    },
    /**
     * @name SuperGrid#_renderAutoHeight
     * @description If enabled the container which holds the supergrid must have a height set on it. Based on this
     * explicit height supergrid will calculate the height of the body based on
     * the containers size minus the height of the footer and the header.
     * @private
     * @function
     */
    _renderAutoHeight: function () {
        var selfHeight = this.element.height(),
            headerHeight = this.element.find('.supergrid_header').height(),
            footerHeight = this.element.find('.supergrid_footer').height();
        this.options.removeHeight = (!this.options.removeHeight) ? 0 : null;
        this.element.find('.supergrid_body')
            .height(selfHeight - ((headerHeight + footerHeight) - this.options.autoHeight.removeHeight));
    },

    /**
     * @name SuperGrid#_sortData
     * @description After sort the data anytime we render the grid(before the render of the grid)
     * @private
     * @function
     */
    _sortData: function () {
        var sortObj = this._getColumnToSortBy(),
            blnAsc,
            customSort,
            getSortValue,
            field;
        if ($.isEmptyObject(sortObj)) {
            return false;
        }
        blnAsc = sortObj.sort === 'asc';
        field = sortObj.field;
        $.each(this.options.columns, function (i, col) {
            if (col.id === field) {
                getSortValue = col.getSortValue;
                customSort = col.sortFunc;
                return false;
            }
        });
        if (customSort) {
            this.options.data.sort(function (a, b) {
                return customSort(a, b, blnAsc);
            });
            return;
        }
        $.extend(this.options.data, simpleSort(this.options.data, 0, this.options.data.length - 1, field, blnAsc));

        function simpleSort(arr, left, right, field, blnAsc) {
            var i = left;
            var j = right;
            var tmp,
                pivotIndex = (left + right) / 2;
            var pivot = arr[pivotIndex.toFixed()][field];
            /*Partition*/
            while (i <= j) {
                if (blnAsc) {
                    while (arr[i][field] < pivot) {
                        i++;
                    }
                    while (arr[j][field] > pivot) {
                        j--;
                    }
                } else { //asc order vs desc order check
                    while (arr[i][field] > pivot) {
                        i++;
                    }
                    while (arr[j][field] < pivot) {
                        j--;
                    }
                }
                if (i <= j) {
                    tmp = arr[i];
                    arr[i] = arr[j];
                    arr[j] = tmp;
                    i++;
                    j--;
                }
            }
            /*Recursion*/
            if (left < j) {
                simpleSort(arr, left, j, field, blnAsc);
            }
            if (i < right) {
                simpleSort(arr, i, right, field, blnAsc);
            }
            return arr;
        }
    },

    /**
     * @name SuperGrid#_getColumnToSortBy
     * @description Called by the sorting data function to determine which column to sort by(uses data id's to know)
     * @private
     * @function
     * @returns {Object} Object containing the id of the column to sort by
     */
    _getColumnToSortBy: function () {
        var sortObj = {};
        $.each(this.options.columns, function (i, col) {
            if (col.sort) {
                sortObj.field = col.id;
                sortObj.sort = col.sort;
                return false;
            }
        });
        return sortObj;
    },

    /**
     * @private
     * @function
     * @name SuperGrid#_pagination
     * @description Determine what the paging settings are
     *
     */
    _pagination: function () {
        var page = this.options.pagination;
        //If Paging is disabled compute only one page and return
        if (!this.options.paginate) {
            page.pageSize = (this.options.data.length > 0) ? this.options.data.length : 0;
            page.currentPage = 1;
            page.numberOfPages = 1;
            page.startIndex = 0;
            page.endIndex = page.pageSize;
            return;
        }
        //Compute Paging

        page.numberOfPages = Math.ceil(this.options.data.length / page.pageSize);
        if (page.currentPage < 1) {
            page.currentPage = 1;
        }
        if (page.currentPage > page.numberOfPages) {
            page.currentPage = page.numberOfPages;
        }
        page.startIndex = (page.currentPage - 1) * page.pageSize;
        if (page.startIndex > (this.options.data.length - 1)) {
            page.startIndex = this.options.data.length - 1;
        }
        page.endIndex = page.startIndex + page.pageSize;
        page.numberOfPages = Math.ceil(this.options.data.length / page.pageSize);
    },

    /**
     * @private
     * @function
     * @name SuperGrid#_buildGrid
     * @description Build grid markup on to stateful array and push it into the DOM
     * {@link SuperGrid#_buildHeader}
     * {@link SuperGrid@_buildBody}
     */
    _buildGrid: function () {
        this.options._grid.push('<div class="supergrid">');

        //determine what kind of grid to build:
        //Either build one for the blind or a full featured UI one.
        if (!this.options.accessibility) {
            this._buildHeader();
            this._buildBody();
            if (this.options.paginate) {
                this._buildFooter();
            }
        } else {
            this.options.paginate = false;
            this._pagination();
            this.options._grid.push('<table class="supergrid_table" style="width:100%;table-layout: fixed;">');
            this._buildAccessibilityHeader();
            this._buildAccessibilityBody();

            this.options._grid.push('</table>');
            this.element.css('overflow', 'scroll');
            this.element.css('height', '500px');
        }
        this.options._grid.push('</div>');
    },

    /**
     * @private
     * @function
     * @name SuperGrid#_buildHeader
     * @description Build grid markup for header based on fixed option
     */
    _buildHeader: function () {
        var widthTotal = 0;
        this.options._grid.push('<div class="supergrid_header">');

        $.each(this.options.columns, function (i, col) {
            var cellClass = col.cellClass || '',
                width = col.width || '',
                id = col.id || '',
                name = col.name || '',
                sort = col.sort || '',
                sortable = col.sortable || '',
                cellStr = '';

            cellStr += '<div style="width:' + col.width + 'px;" scope="col" class="supergrid_cell ' + cellClass + '" data-id="' + id + '" tabIndex="0" ';
            if (sort) {
                cellStr += 'data-sort="' + sort + '" ';
            }
            cellStr += 'data-sortable="' + sortable + '">';
            cellStr += '<div>';
            cellStr += name;
            if (sort) {
                cellStr += '<div class="sort-icon"></div>';
            }
            cellStr += '</div>';
            cellStr += '</div>';
            cellStr += '<div class="resize-handle" data-id="' + id + '"data-diff="' + widthTotal + '"></div>';
            widthTotal += width;
            this.options._grid.push(cellStr);
        }.bind(this));

        this.options._grid.push('</div>');
    },

    /**
     * @name SuperGrid#_updateHeader
     * @description Needs refactoring.
     * @private
     * @function
     * @param {Number} colId
     * @param {Number} colWidth
     */
    _updateHeader: function (colId, colWidth) {
        var context = this;
        var widthTotal = 0;

        $.each(this.options.columns, function (i, col) {
            var cellClass = col.cellClass || '',
                width = (colId === col.id) ? col.width = colWidth : col.width || '',
                id = col.id || '',
                name = col.name || '',
                sort = col.sort || '',
                sortable = col.sortable || '',
                cellStr = '';

            cellStr += '<div style="width:' + col.width + 'px;" scope="col" class="supergrid_cell ' + cellClass + '" data-id="' + id + '" tabIndex="0" ';
            if (sort) {
                cellStr += 'data-sort="' + sort + '" ';
            }
            cellStr += 'data-sortable="' + sortable + '">';
            cellStr += '<div>';
            cellStr += name;
            if (sort) {
                cellStr += '<div class="sort-icon"></div>';
            }
            cellStr += '</div>';
            cellStr += '</div>';
            cellStr += '<div class="resize-handle" data-id="' + id + '"data-diff="' + widthTotal + '"></div>';
            widthTotal += width;
            context.options._header.push(cellStr);
        });

        context.element.find('.supergrid_header').html(this.options._header.join(''));
        context.options._header = [];
    },

    /**
     * @private
     * @function
     * @name SuperGrid#_buildBody
     * @description Build grid markup for body
     */
    _buildBody: function () {
        var data = this.options.data,
            columns = this.options.columns,
            context = this;
        this.options._grid.push('<div class="supergrid_body">');

        $.each(data.slice(this.options.pagination.startIndex, this.options.pagination.endIndex), buildRow);

        this.options._grid.push('</div>');

        //noinspection JSUnusedLocalSymbols
        function buildRow(i, dataSet) {
            var id = dataSet.id || '',
                row = '';
            row += '<div class="supergrid_row section" data-id="' + id + '">';
            $.each(columns, function (i, col) {
                var cellClass = col.cellClass || '';
                row += '<div style="width:' + col.width + 'px;" class="supergrid_cell ' + cellClass + '" tabIndex="0"' +
                    'data-id="' + col.id + '">';
                row += '<div>';
                row += buildCell(dataSet, col);
                row += '</div>';
                row += '</div>';
            });
            row += '</div>';
            context.options._grid.push(row);
        }

        function buildCell(data, column) {
            var attributes = [],
                regex = /\#(.*?)\#/,
                formatter = column.formatter,
                formatterHelper,
                matchedAttr;
            if (!data) {
                return '';
            }
            if (typeof formatter === 'function') {
                return formatter(data);
            }
            if (typeof formatter === 'object') {
                formatter = column.formatter;
            }

            formatterHelper = formatter;
            matchedAttr = regex.exec(formatter);
            if (formatterHelper) {
                while (matchedAttr) {
                    attributes.push(matchedAttr[0]);
                    formatterHelper = formatterHelper.replace(matchedAttr[0], '');
                    matchedAttr = regex.exec(formatterHelper);
                }
                $.each(attributes, function (index, attr) {
                    var value = attr.replace(/#|_/g, '');
                    formatter = formatter.replace(attr, data[value]);
                });
                return formatter;
            }
            return data[column.id];
        }
    },

    /**
     * @private
     * @function
     * @name SuperGrid#_buildFooter
     * @description Build grid markup for paging footer
     */
    _buildFooter: function () {
        var grid = this.options._grid;
        grid.push('<div class="supergrid_footer">');
        grid.push('<div class="pager">');
        grid.push(' <button class="paginate left" tabindex="0"><i></i><i></i></button>');
        grid.push(' <div class="counter"></div>');
        grid.push(' <button class="paginate right" tabindex="0"><i></i><i></i></button>');
        grid.push('</div>');
        grid.push('</div>');
    },

    /**
     * @private
     * @function
     * @name SuperGrid#_buildAccessibilityHeader
     * @description Build grid markup for header based on html5 table for blind and aria users
     */
    _buildAccessibilityHeader: function () {
        var widthTotal = 0;
        this.options._grid.push('<thead class="supergrid_header"><tr>');
        $.each(this.options.columns, function (i, col) {
            var cellClass = col.cellClass || '',
                width = col.width || '',
                id = col.id || '',
                name = col.name || '',
                sort = col.sort || '',
                sortable = col.sortable || '',
                cellStr = '';

            cellStr += '<th style="width:' + col.width + 'px;" scope="col" class="supergrid_cell ' + cellClass + '" data-id="' + id + '" tabIndex="0" ';
            if (sort) {
                cellStr += 'data-sort="' + sort + '" ';
            }
            cellStr += 'data-sortable="' + sortable + '">';
            cellStr += '<div>';
            cellStr += name;
            if (sort) {
                cellStr += '<div class="sort-icon"></div>';
            }
            cellStr += '</div>';
            cellStr += '</th>';
            //cellStr += '<div class="resize-handle" data-id="' + id + '"data-diff="' + widthTotal + '"></div>';
            widthTotal += width;
            this.options._grid.push(cellStr);
        }.bind(this));

        this.options._grid.push('</tr></thead>');
    },

    /**
     * @private
     * @function
     * @name SuperGrid#_buildAccessibilityBody
     * @description Build grid markup for body based on html5 table for blind and aria users
     */
    _buildAccessibilityBody: function () {
        var data = this.options.data,
            columns = this.options.columns,
            context = this;
        this.options._grid.push('<tbody class="supergrid_body">');

        $.each(data.slice(this.options.pagination.startIndex, this.options.pagination.endIndex), buildRow);

        this.options._grid.push('</tbody>');

        //noinspection JSUnusedLocalSymbols
        function buildRow(i, dataSet) {
            var id = dataSet.id || '',
                row = '';
            row += '<tr class="supergrid_row section" data-id="' + id + '">';
            $.each(columns, function (i, col) {
                var cellClass = col.cellClass || '';
                row += '<td style="width:' + col.width + 'px;" class="supergrid_cell ' + cellClass + '" tabIndex="0"' +
                    'data-id="' + col.id + '">';
                row += '<div>';
                row += buildCell(dataSet, col);
                row += '</div>';
                row += '</td>';
            });
            row += '</tr>';
            context.options._grid.push(row);
        }

        function buildCell(data, column) {
            var attributes = [],
                regex = /\#(.*?)\#/,
                formatter = column.formatter,
                formatterHelper,
                matchedAttr;
            if (!data) {
                return '';
            }
            if (typeof formatter === 'function') {
                return formatter(data);
            }
            if (typeof formatter === 'object') {
                formatter = column.formatter;
            }

            formatterHelper = formatter;
            matchedAttr = regex.exec(formatter);
            if (formatterHelper) {
                while (matchedAttr) {
                    attributes.push(matchedAttr[0]);
                    formatterHelper = formatterHelper.replace(matchedAttr[0], '');
                    matchedAttr = regex.exec(formatterHelper);
                }
                $.each(attributes, function (index, attr) {
                    var value = attr.replace(/#|_/g, '');
                    formatter = formatter.replace(attr, data[value]);
                });
                return formatter;
            }
            return data[column.id];
        }
    },

    /**
     * @name SuperGrid#_updatePages
     * @description Updates the pagination display with the latest current page and number of pages
     * @private
     * @function
     * @param {number} currentPage Current page being rendered.
     */
    _updatePages: function (currentPage) {
        var $counter = this.element.find('.counter'),
            $leftArrow = this.element.find('.paginate.left'),
            $rightArrow = this.element.find('.paginate.right');
        if (currentPage <= 1) {
            $leftArrow.attr('data-state', 'disabled');
        }
        if (currentPage === this.options.pagination.numberOfPages) {
            $rightArrow.attr('data-state', 'disabled');
        }
        $counter.html(currentPage + '/' + this.options.pagination.numberOfPages);
    },

    /**
     * @name SuperGrid#_addMetaData
     * @description After rendering the grid, attach meta data to the the row by id
     * @private
     * @function
     */
    _addMetaData: function () {
        var context = this;
        $.each(this.options.data, function (i, row) {
            context.element.find('supergrid_row[data-id="' + row.id + '"]').data(row);
        });
    },

    /**
     * @name SuperGrid#updateGrid
     * @description Updates the table with new data and optionally new column configs.
     * @function
     * @param {Array} data New Data to push to the grid.
     * @param {Array} columns (optional)
     * {@link SuperGrid#_renderGrid}
     */
    updateGrid: function (data, columns) {
        data && (this.options.data = $.extend([], data));
        columns && (this.options.columns = $.extend([], columns));
        this.element.empty();
        this._renderGrid();
    }
});