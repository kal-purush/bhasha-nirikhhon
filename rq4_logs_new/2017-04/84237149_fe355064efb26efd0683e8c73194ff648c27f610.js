"use strict;"
var myDropzone = undefined;
var filesAdded = false;

$.fn.exists = function () {
    return this.length !== 0;
}

var initPhotoSwipeFromDOM = function (gallerySelector) {

    // parse slide data (url, title, size ...) from DOM elements 
    // (children of gallerySelector)
    var parseThumbnailElements = function (el) {
        var thumbElements = el.childNodes,
            numNodes = thumbElements.length,
            items = [],
            figureEl,
            linkEl,
            size,
            item;

        for (var i = 0; i < numNodes; i++) {

            figureEl = thumbElements[i]; // <figure> element

            // include only element nodes 
            if (figureEl.nodeType !== 1) {
                continue;
            }

            linkEl = figureEl.children[0]; // <a> element

            size = linkEl.getAttribute('data-size').split('x');

            // create slide object
            item = {
                src: linkEl.getAttribute('href'),
                w: parseInt(size[0], 10),
                h: parseInt(size[1], 10)
            };



            if (figureEl.children.length > 1) {
                // <figcaption> content
                item.title = figureEl.children[1].innerHTML;
            }

            if (linkEl.children.length > 0) {
                // <img> thumbnail element, retrieving thumbnail url
                item.msrc = linkEl.children[0].getAttribute('src');
            }

            item.el = figureEl; // save link to element for getThumbBoundsFn
            items.push(item);
        }

        return items;
    };

    // find nearest parent element
    var closest = function closest(el, fn) {
        return el && (fn(el) ? el : closest(el.parentNode, fn));
    };

    // triggers when user clicks on thumbnail
    var onThumbnailsClick = function (e) {
        e = e || window.event;
        e.preventDefault ? e.preventDefault() : e.returnValue = false;

        var eTarget = e.target || e.srcElement;

        // find root element of slide
        var clickedListItem = closest(eTarget, function (el) {
            return (el.tagName && el.tagName.toUpperCase() === 'FIGURE');
        });

        if (!clickedListItem) {
            return;
        }

        // find index of clicked item by looping through all child nodes
        // alternatively, you may define index via data- attribute
        var clickedGallery = clickedListItem.parentNode,
            childNodes = clickedListItem.parentNode.childNodes,
            numChildNodes = childNodes.length,
            nodeIndex = 0,
            index;

        for (var i = 0; i < numChildNodes; i++) {
            if (childNodes[i].nodeType !== 1) {
                continue;
            }

            if (childNodes[i] === clickedListItem) {
                index = nodeIndex;
                break;
            }
            nodeIndex++;
        }



        if (index >= 0) {
            // open PhotoSwipe if valid index found
            openPhotoSwipe(index, clickedGallery);
        }
        return false;
    };

    // parse picture index and gallery index from URL (#&pid=1&gid=2)
    var photoswipeParseHash = function () {
        var hash = window.location.hash.substring(1),
        params = {};

        if (hash.length < 5) {
            return params;
        }

        var vars = hash.split('&');
        for (var i = 0; i < vars.length; i++) {
            if (!vars[i]) {
                continue;
            }
            var pair = vars[i].split('=');
            if (pair.length < 2) {
                continue;
            }
            params[pair[0]] = pair[1];
        }

        if (params.gid) {
            params.gid = parseInt(params.gid, 10);
        }

        return params;
    };

    var openPhotoSwipe = function (index, galleryElement, disableAnimation, fromURL) {
        var pswpElement = document.querySelectorAll('.pswp')[0],
            gallery,
            options,
            items;

        items = parseThumbnailElements(galleryElement);

        // define options (if needed)
        options = {

            // define gallery index (for URL)
            galleryUID: galleryElement.getAttribute('data-pswp-uid'),

            getThumbBoundsFn: function (index) {
                // See Options -> getThumbBoundsFn section of documentation for more info
                var thumbnail = items[index].el.getElementsByTagName('img')[0], // find thumbnail
                    pageYScroll = window.pageYOffset || document.documentElement.scrollTop,
                    rect = thumbnail.getBoundingClientRect();

                return { x: rect.left, y: rect.top + pageYScroll, w: rect.width };
            }

        };

        // PhotoSwipe opened from URL
        if (fromURL) {
            if (options.galleryPIDs) {
                // parse real index when custom PIDs are used 
                // http://photoswipe.com/documentation/faq.html#custom-pid-in-url
                for (var j = 0; j < items.length; j++) {
                    if (items[j].pid == index) {
                        options.index = j;
                        break;
                    }
                }
            } else {
                // in URL indexes start from 1
                options.index = parseInt(index, 10) - 1;
            }
        } else {
            options.index = parseInt(index, 10);
        }

        // exit if index not found
        if (isNaN(options.index)) {
            return;
        }

        if (disableAnimation) {
            options.showAnimationDuration = 0;
        }

        // Pass data to PhotoSwipe and initialize it
        gallery = new PhotoSwipe(pswpElement, PhotoSwipeUI_Default, items, options);
        gallery.init();
    };

    // loop through all gallery elements and bind events
    var galleryElements = document.querySelectorAll(gallerySelector);

    for (var i = 0, l = galleryElements.length; i < l; i++) {
        galleryElements[i].setAttribute('data-pswp-uid', i + 1);
        galleryElements[i].onclick = onThumbnailsClick;
    }

    // Parse URL and open gallery if it contains #&pid=3&gid=1
    var hashData = photoswipeParseHash();
    if (hashData.pid && hashData.gid) {
        openPhotoSwipe(hashData.pid, galleryElements[hashData.gid - 1], true, true);
    }
};

function setAddImagesToCategoryDropzone() {
    // Get the template HTML and remove it from the doumenthe template HTML and remove it from the doument
    var previewNode = document.querySelector("#template");
    previewNode.id = "";
    var previewTemplate = previewNode.parentNode.innerHTML;
    previewNode.parentNode.removeChild(previewNode);

    myDropzone = new Dropzone(document.body, { // Make the whole body a dropzone
        url: "AddImagesToCategory", // Set the url
        thumbnailWidth: 80,
        thumbnailHeight: 80,
        parallelUploads: 20,
        previewTemplate: previewTemplate,
        autoQueue: false, // Make sure the files aren't queued until manually added
        previewsContainer: "#previews", // Define the container to display the previews
        clickable: ".fileinput-button" // Define the element that should be used as click trigger to select files.
    });

    myDropzone.on("addedfile", function (file) {
        $("#actions .start").css("display", "inline-block");
        $("#actions .cancel").css("display", "inline-block");
    });

    myDropzone.on("removedfile", function (file) {
        if (myDropzone.files.length === 0) {
            $("#actions .start").css("display", "none");
            $("#actions .cancel").css("display", "none");
        }
    });

    myDropzone.on("complete", function () {
        if (this.getQueuedFiles().length === 0 &&
            this.getUploadingFiles().length === 0) {
            $("#actions .start").css("display", "none");
            $("#actions .cancel").css("display", "none");
        }
        filesAdded = true;
    });

    document.querySelector("#actions .start").onclick = function () {
        myDropzone.enqueueFiles(myDropzone.getFilesWithStatus(Dropzone.ADDED));
    };
    document.querySelector("#actions .cancel").onclick = function () {
        myDropzone.removeAllFiles(true);
    };
}

function setActiveNavigationItem() {
    var url = window.location.href;

    $("main-nav-item").each(function () {
        $(this).removeClass("active");
    });

    if (url.indexOf("Home/Images") > -1) {
        $("#main-nav-item-1").addClass("active");
    } else if (url.indexOf("Home/Services") > -1) {
        $("#main-nav-item-2").addClass("active");
    } else if (url.indexOf("Home/Contact") > -1) {
        $("#main-nav-item-3").addClass("active");
    } else if (url.indexOf("Home/Features") > -1) {
        $("#main-nav-item-4").addClass("active");
    }
}

$(document).ready(function () {

    setActiveNavigationItem();

    setCategoryButtons();

    setServicesTypeButtons();

    setServicesButtons();

    setThumbnailButtons();

    setFeatureButtons();

    $('.delete-images-from-category').on('click', function (event) {
        $('#all-images-category-link').attr("href", $('#all-images-category-link-input').attr("value") + '?categoryId=' + $(this).attr("data-image-id"));
        $('#all-images-category-modal').modal();
    });

    $('.delete-img-from-category').on('click', function (event) {
        var categoryId = $(event.target).parent().parent().parent().parent().parent().parent().attr("data-category-id");
        $('#delete-image-category-link').attr("href", $('#delete-image-category-link-input').attr("value") + '?categoryId=' + categoryId + '&imageId=' + $(this).attr("data-image-id"));
        $('#delete-image-category-modal').modal();
    });

    $('.delete-feature').on('click', function (event) {
        $('#delete-feature-link').attr("href", $('#delete-feature-link-input').attr("value") + '?id=' + $(this).attr("data-feature-id"));
        $('#delete-feature-modal').modal();
    });

    if ($("#add-images-category-modal").exists()) {
        $('#add-images-category-modal').on('hidden.bs.modal', function (e) {
            myDropzone.removeAllFiles();
            if (filesAdded)
                location.reload();
        })
    }

    if ($(".add-image-btn").exists()) {
        $('.add-image-btn').on('click', function (event) {
            if (!myDropzone)
                setAddImagesToCategoryDropzone();

            myDropzone.options.url = "AddImagesToCategory?categoryId=" + $(event.target).attr("data-category-id");

            filesAdded = false;
            $('#add-images-category-modal').modal();
        });
    }

    if ($(".toggle-images-category").exists()) {
        $(".toggle-images-category").on('click', function (event) {
            var target = undefined;

            if ($(event.target).hasClass('glyphicon'))
                target = $(event.target);
            else if ($(event.target).hasClass('toggle-images-category'))
                target = $(event.target).children('.glyphicon');

            if (target) {
                if ($(target).hasClass("glyphicon-menu-down")) {
                    $(target).parent().parent().children(".row").addClass("zero-height");
                    $(target).parent().parent().children("span").addClass("zero-height");
                    $(target).addClass("glyphicon-menu-up").removeClass("glyphicon-menu-down");
                } else if ($(target).hasClass("glyphicon-menu-up")) {
                    $(target).parent().parent().children(".row").removeClass("zero-height");
                    $(target).parent().parent().children("span").removeClass("zero-height");
                    $(target).addClass("glyphicon-menu-down").removeClass("glyphicon-menu-up");
                }
            }
        });
    }

    // execute above function
    if ($(".images-gallery").exists()) {
        initPhotoSwipeFromDOM('.images-gallery');
    }

    if ($(".slider-for").exists()) {
        $('.slider-for').slick({
            slidesToShow: 1,
            slidesToScroll: 1,
            arrows: true,
            fade: true,
            dots: true,
            adaptiveHeight: true
        });
    }
});

function setThumbnailButtons() {

    $('.edit-image-btn').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                var visibles = $(this).parent().parent().children('.display-block');
                var hiddens = $(this).parent().parent().children('.display-none');

                $(visibles).addClass('display-none').removeClass('display-block');
                $(hiddens).addClass('display-block').removeClass('display-none');
                $(this).parent().parent().parent().children('.img-wrapper').addClass('display-none');
            });
        }
    });

    $('.cancel-edit-image-btn').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                var visibles = $(this).parent().parent().children('.display-block');
                var hiddens = $(this).parent().parent().children('.display-none');
                $(visibles).addClass('display-none').removeClass('display-block');
                $(hiddens).addClass('display-block').removeClass('display-none');
                $(this).parent().parent().parent().children('.img-wrapper').removeClass('display-none');
            });
        }
    });

    $('.add-image-category-btn').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                var disable_categories = $(this).attr("data-categories-id").split(" ");
                var image_id = $(this).attr("data-image-id");
                $("#add-image-to-category-id").attr("value", image_id);
                $('.category-checkbox').attr('disabled', false);

                for (var i = 0; i < disable_categories.length; i++){
                    var selector = '#checkbox_' + disable_categories[i];
                    $(selector).attr('disabled', true);
                }

                $('.category-checkbox').on('change', function (event) {
                    if ($('.category-checkbox:checked').length > 0) {
                        $('#add-image-category-link').css("display", "inline-block");
                    }
                    else
                        $('#add-image-category-link').css("display", "none");
                });

                $('#add-category-image-modal').modal();
            });
        }
    });

}

function setFeatureButtons() {

    $('.edit-feature-btn').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                var visibles = $(this).parent().parent().children('.display-block');
                var hiddens = $(this).parent().parent().children('.display-none');

                $(visibles).addClass('display-none').removeClass('display-block');
                $(hiddens).addClass('display-block').removeClass('display-none');
                $(this).parent().parent().parent().children('.img-wrapper').addClass('display-none');
            });
        }
    });

    $('.cancel-edit-feature-btn').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                var visibles = $(this).parent().parent().children('.display-block');
                var hiddens = $(this).parent().parent().children('.display-none');
                $(visibles).addClass('display-none').removeClass('display-block');
                $(hiddens).addClass('display-block').removeClass('display-none');
                $(this).parent().parent().parent().children('.img-wrapper').removeClass('display-none');
            });
        }
    });
}

function setCategoryButtons() {

    $('.edit-category-btn').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                $(this).parent().parent().children('.display-none').removeClass('display-none');
                $(this).parent().parent().children().eq(0).addClass('display-none');
            });
        }
    });

    $('.cancel-edit-category').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                $(this).parent().parent().children('.display-none').removeClass('display-none');
                $(this).parent().parent().children().eq(1).addClass('display-none');
            });
        }
    });

    $('.delete-category').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                $('#delete-category-modal-link').attr("href", $('#delete-category-modal-input').attr("value") + "/" + $(this).attr("id"));
                $('#delete-category-modal').modal();
            });
        }
    });
}

function setServicesTypeButtons() {

    $('.edit-service-type-btn').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                $(this).parent().parent().children('.display-none').removeClass('display-none');
                $(this).parent().parent().children().eq(0).addClass('display-none');
            });
        }
    });

    $('.cancel-edit-service-type').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                $(this).parent().parent().children('.display-none').removeClass('display-none');
                $(this).parent().parent().children().eq(1).addClass('display-none');
            });
        }
    });

    $('.delete-service-type').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                $('#delete-service-type-modal-link').attr("href", $('#delete-service-type-modal-input').attr("value") + "/" + $(this).attr("id"));
                $('#delete-service-type-modal').modal();
            });
        }
    });
}

function setServicesButtons() {

    $('.edit-service-btn').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                $(this).parent().parent().children('.display-none').removeClass('display-none');
                $(this).parent().parent().children().eq(0).addClass('display-none');
            });
        }
    });

    $('.cancel-edit-service').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                $(this).parent().parent().children('.display-none').removeClass('display-none');
                $(this).parent().parent().children().eq(1).addClass('display-none');
            });
        }
    });

    $('.delete-service').each(function (elem) {
        if (!isOnClickBinded($(this))) {
            $(this).on('click', function (event) {
                $('#delete-service-modal-link').attr("href", $('#delete-service-modal-input').attr("value") + "/" + $(this).attr("id"));
                $('#delete-service-modal').modal();
            });
        }
    });

    $.validator.unobtrusive.parse(".edit-service-form");
}

function isOnClickBinded(elem) {
    var result = false;
    var $events = jQuery._data(elem[0], "events");

    if (typeof $events != "undefined") {
        jQuery.each($events, function (i, event) {
            jQuery.each(event, function (i, event) {
                if (event.type && event.type === "click" && result === false) {
                    result = true;
                }
            });
        });
    }

    return result;

}