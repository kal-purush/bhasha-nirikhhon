/*jslint this:true*/
/*globals jQuery*/
/**
 * Lightweight jQuery plugin that creates a tab canvas.
 *
 * @author Dan Bettles <dan@powder-blue.com>
 * @copyright Powder Blue Ltd 2015
 * @license MIT
 */

(function () {
    'use strict';

    /**
     * @class
     * @param {jQuery} $el
     * @param {Object} options
     */
    function Tabby($el, options) {
        var finalOptions;

        finalOptions = jQuery.extend({
            createTabs: false,  //By default, look for existing tabs in the markup.
            select: jQuery.noop  //This is for use only by *our* very old code.
        }, options || {});

        this.setEl($el);
        this.setOptions(finalOptions);

        this.setTabsEl(undefined);
    }

    /**
     * CSS class applied to selected elements.
     *
     * @type String
     */
    Tabby.CSS_CLASS_SELECTED = 'tabby-selected';

    Tabby.prototype = {

        /**
         * @private
         * @param {jQuery} $el
         * @returns {Tabby}
         */
        setEl: function ($el) {
            this.$el = $el;
            return this;
        },

        /**
         * @returns {jQuery}
         */
        getEl: function () {
            return this.$el;
        },

        /**
         * @private
         * @param {Object} options
         * @returns {Tabby}
         */
        setOptions: function (options) {
            this.options = options;
            return this;
        },

        /**
         * @returns {Object}
         */
        getOptions: function () {
            return this.options;
        },

        /**
         * @private
         * @returns {jQuery}
         */
        findTabsEl: function () {
            return this.getEl().children('ul, ol').first();
        },

        /**
         * @private
         * @returns {Tabby}
         */
        setTabsEl: function ($tabs) {
            this.$tabs = $tabs;
            return this;
        },

        /**
         * @private
         * @returns {jQuery}
         */
        getTabsEl: function () {
            return this.$tabs;
        },

        /**
         * @private
         * @returns {jQuery}
         */
        getTabEls: function () {
            return this.getTabsEl().children();
        },

        /**
         * @private
         * @returns {jQuery}
         */
        getPageEls: function () {
            return this.getEl().children().not(this.getTabsEl());
        },

        /**
         * @private
         * @param {jQuery} $selectedTab
         * @returns {Tabby}
         */
        tabClick: function ($selectedTab) {
            var selectedTabNo,
                $selectedPage,
                pageSelector;

            pageSelector = $selectedTab.data('tabby-page-selector');

            if (pageSelector === undefined) {
                selectedTabNo = $selectedTab.index();

                this.getPageEls().each(function (currPageNo) {
                    if (currPageNo === selectedTabNo) {
                        $selectedPage = jQuery(this);
                        return false;
                    }

                    return;
                });
            } else {
                $selectedPage = this.getEl().find(pageSelector);
            }

            if (!($selectedPage && $selectedPage.size())) {
                throw 'Failed to determine the page to select.';
            }

            //Select the page.
            this.getPageEls().removeClass(Tabby.CSS_CLASS_SELECTED);
            $selectedPage.addClass(Tabby.CSS_CLASS_SELECTED);

            //Select the tab.
            this.getTabEls().removeClass(Tabby.CSS_CLASS_SELECTED);
            $selectedTab.addClass(Tabby.CSS_CLASS_SELECTED);

            //Call user event-handlers.
            this.getOptions().select.call(this, undefined, {panel: $selectedPage});

            return this;
        },

        /**
         * Builds the GUI.
         *
         * @returns {Tabby}
         */
        setUp: function () {
            var tabby = this,
                $tabs;

            if (!this.getOptions().createTabs) {
                $tabs = this.findTabsEl();

                if (!$tabs.size()) {
                    throw 'Failed to find tabs.';
                }

                this.setTabsEl($tabs);
            }

            if (!(this.getTabsEl() && this.getTabsEl().size())) {
                throw 'There are no tabs.';
            }

            this.getEl().addClass('tabby');

            this.getTabsEl().addClass('tabby-tabs');

            this.getTabEls().each(function () {
                var $tab = jQuery(this),
                    $anchors,
                    firstAnchorHref,
                    matches;

                $anchors = $tab.find('a');

                if ($anchors.size()) {
                    //Disable all links within the tab.
                    $anchors.click(function (e) {
                        e.preventDefault();
                    });

                    firstAnchorHref = $anchors.first().attr('href');
                    matches = firstAnchorHref.match(/^(#.*)$/);

                    //Does the link point to something in the page?
                    if (matches !== null) {
                        //Associate the ID of the referenced element (the tab page, we assume) with the tab.  This'll
                        //make selecting the page easier.
                        $tab.data('tabby-page-selector', matches[1]);
                    }
                }

                $tab
                    //Handle clicks anywhere in the tab.
                    .click(function () {
                        tabby.tabClick(jQuery(this));
                    })
                    .addClass('tabby-tab');
            });

            this.getPageEls().addClass('tabby-page');

            //Kick things off by selecting the first page.
            this.tabClick(this.getTabEls().first());

            return this;
        }
    };

    jQuery.fn.extend({

        /**
         * @param {Object} options
         * @returns {jQuery}
         */
        tabs: function (options) {
            return this.each(function () {
                var $tabCanvas = jQuery(this);

                $tabCanvas
                    .data('tabby', new Tabby($tabCanvas, options))
                    .data('tabby')
                        .setUp();
            });
        }
    });
}());