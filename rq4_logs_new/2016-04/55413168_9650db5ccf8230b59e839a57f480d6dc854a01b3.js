$.noConflict();
var myMask = new Ext.LoadMask(Ext.getBody(), {
    msg: "Please wait..."
});
var label_id = 0;
jQuery(document).ready(function () {
    jQuery(".ui-tabs-panel").sortable();
    var baseUrl = location.href.substr(0, location.href.lastIndexOf("/") + 1);
    //jQuery( document ).tooltip();
    jQuery(".label_layer_name").dblclick(function () {
        myMask.show();
        label_id = jQuery(this).attr("id").split("_");
        label_id = label_id[3];
        jQuery('#hidden_layer_name_' + label_id).show();
        jQuery('#label_layer_name_' + label_id).hide();
        jQuery('#field_layer_name_' + label_id).val(jQuery.trim(jQuery('#label_layer_name_' + label_id).html()));
        myMask.hide();
    });

    jQuery(".label_tab_name").click(function () {
        myMask.show();
        label_id = jQuery(this).attr("id").split("_");
        label_id = label_id[3];
        jQuery('#hidden_tab_name_' + label_id).show();
        jQuery('#label_tab_name_' + label_id).hide();
        jQuery('#field_tab_name_' + label_id).val(jQuery.trim(jQuery('#label_tab_name_' + label_id).html()));
        myMask.hide();
    });


    //added by abin
    jQuery(".reset-parent").hover(
            function () {
                jQuery(this).find(".ls-reset").show();
            },
            function () {
                jQuery(this).find(".ls-reset").hide();
            });

});

function saveLayerLabel(label_id) {
    jQuery('#hidden_layer_name_' + label_id).hide();
    jQuery('#label_layer_name_' + label_id).show();
    var baseUrl = location.href.substr(0, location.href.lastIndexOf("/") + 1);
    var layerName = jQuery('#field_layer_name_' + label_id).val();
    var url = baseUrl + 'mod.php?M=web_PitsLayersliderPitslayerslider&id=3&tx_pitslayerslider_web_pitslayersliderpitslayerslider[action]=saveLabel';
    if (layerName.length !== 0) {
        myMask.show();
        jQuery.ajax({
            url: url,
            type: "POST",
            data: {
                tx_pitslayerslider_web_pitslayersliderpitslayerslider: ({
                    'label_name': layerName,
                    'layer_id': label_id,
                    'type': 'layer'
                })
            },
            success: function (html) {
                jQuery('#label_layer_name_' + label_id).html(layerName);
                myMask.hide();
            }
        });
    }
}

function closeLayerLabel(label_id) {

    jQuery('#hidden_layer_name_' + label_id).hide();
    jQuery('#label_layer_name_' + label_id).show();
    jQuery('#field_layer_name_' + label_id).val(jQuery.trim(jQuery('#label_layer_name_' + label_id).html()));

}
/* tab label edit*/
function removeTab(tabId, type, url) {
    var didConfirm = confirm("Delete " + type + "?");
    if (didConfirm === true) {
        var baseUrl = location.href.substr(0, location.href.lastIndexOf("/") + 1);
//        var url = baseUrl + 'mod.php?M=web_PitsLayersliderPitslayerslider&id=3&tx_pitslayerslider_web_pitslayersliderpitslayerslider[action]=deleteTab';
        jQuery.ajax({
            url: url,
            type: "POST",
            data: {
                tx_pitslayerslider_web_pitslayersliderpitslayerslider: ({
                    'tabId': tabId,
                    'type': type
                })
            },
            success: function (html) {
                location.reload(true);
            }
        });
    }


}
//elfinder both for tab and layer browser combined
var imageId = '';
var elType = 0;
var textBoxId = '';
var sliderId = '';
var layerId = 0;
function load_elfinder(id, type) {
    jQuery('.ui-state-active').css('z-index', 0);
    elType = type;
    imageId = '#child_layer_img_' + id;
    textBoxId = '#background_' + id;
    thumbBoxId = '#thumb_' + id;
    sliderId = 'layer_' + id;
    layerId = id;
    loadPopup();
    jQuery('#elfinder').elfinder({
        url: location.href.substr(0, location.href.lastIndexOf("/") + 1).slice(0,-6) + 'typo3conf/ext/pits_layerslider/Configuration/FileFinder2/connectorNew.php',
        dialog: {
            width: 900,
            modal: true
        },
        rememberLastDir: false,
        getFileCallback: function (url) {
            getUrl(url);
            disablePopup();
        },
        uiOptions: {
            toolbar: [
                ['back', 'forward'],
                ['mkdir', 'mkfile', 'upload'],
                ['open', 'download', 'getfile']
            ]
        }
    });
}
function getUrl(urlObj) {
    if (elType === 1) {
        jQuery('#' + sliderId).remove();
        jQuery(imageId).attr('src', urlObj.url);
        jQuery(imageId + '_effect').attr('src', urlObj.url);
        jQuery(imageId + '_text').val(urlObj.url);
        jQuery('#ls-preview-content').show();
        jQuery('<img/>', {
            id: sliderId,
            src: urlObj.url
        }).appendTo('#ls-preview-content')
                .addClass("draggable");
        doDraggableAction();

    }
    if (elType === 3) {
        jQuery(thumbBoxId).val(urlObj.url);
    }
    if (elType === 0) {
        jQuery(textBoxId).val(urlObj.url);
        jQuery('#ls-preview-content').show();
        jQuery('#' + sliderId).attr('src', urlObj.url);
        var backGroundImage = '<img src="' + urlObj.url + '" id="' + sliderId + '" alt="Slide background" class="ls-bg background" >';
        jQuery('.ui-state-active').css('z-index', 999);
        jQuery('#layerslider').layerSlider({
            skin: 'preview',
            autoStart: false,
            responsive: false,
            skinsPath: '../typo3conf/ext/pits_layerslider/Resources/Public/layerslider/skins/',
            thumbnailNavigation: 'hover',
            hoverPrevNext: false,
            autoPlayVideos: false,
            navPrevNext: true,
            navStartStop: true,
            navButtons: true,
            cbInit: function (element) {
                jQuery('#layerslider').layerSlider("stop");
            }
        });
    }

}

/*function getBaseURL() {
 var url = location.href;  // entire url including querystring - also: window.location.href;
 var baseURL = url.substring(0, url.indexOf('/', 14));
 if (baseURL.indexOf('http://localhost') != -1 ) {
 // Base Url for localhost
 var url = location.href;  // window.location.href;
 var pathname = location.pathname;  // window.location.pathname;
 var index1 = url.indexOf(pathname);
 var index2 = url.indexOf("/", index1 + 1);
 var baseLocalUrl = url.substr(0, index2);
 return baseLocalUrl + "/";
 }
 else {
 // Root Url for domain name
 return baseURL + "/";
 }
 }*/

function saveTabLabel(label_id) {
    jQuery('#hidden_tab_name_' + label_id).hide();
    jQuery('#label_tab_name_' + label_id).show();
    var baseUrl = location.href.substr(0, location.href.lastIndexOf("/") + 1);
    var tabName = jQuery('#field_tab_name_' + label_id).val();
    var url = baseUrl + 'mod.php?M=web_PitsLayersliderPitslayerslider&id=3&tx_pitslayerslider_web_pitslayersliderpitslayerslider[action]=saveLabel';
    if (tabName.length !== 0) {
        myMask.show();
        jQuery.ajax({
            url: url,
            type: "POST",
            data: {
                tx_pitslayerslider_web_pitslayersliderpitslayerslider: ({
                    'label_name': tabName,
                    'tab_id': label_id,
                    'type': 'tab'
                })
            },
            success: function (html) {
                jQuery('#label_tab_name_' + label_id).html(tabName);
                jQuery('#tab_id_' + label_id).html(tabName);
                myMask.hide();
            }
        });
    }
}

function closeTabLabel(label_id) {

    jQuery('#hidden_tab_name_' + label_id).hide();
    jQuery('#label_tab_name_' + label_id).show();
    jQuery('#field_tab_name_' + label_id).val(jQuery.trim(jQuery('#label_tab_name_' + label_id).html()));

}

function setPreview(layerId) {
    topVal = jQuery('#layer_top_' + layerId).val();
    leftVal = jQuery('#layer_left_' + layerId).val();
    jQuery('#ls-preview-content').find('#layer_' + layerId).css('top', topVal);
    jQuery('#ls-preview-content').find('#layer_' + layerId).css('left', leftVal);
}

function doDraggableAction() {
    jQuery(".draggable").draggable({
        scroll: false,
        drag: function () {
            var domRecord = jQuery(this).attr("id").split("_");
            var domId = domRecord[1];
            var topValue = jQuery(this).css("top").replace("px", "");
            var leftValue = jQuery(this).css("left").replace("px", "");
            jQuery("#layer_top_" + domId).val(topValue);
            jQuery("#layer_left_" + domId).val(leftValue);
        },
        stop: function () {
            var domRecord = jQuery(this).attr("id").split("_");
            var domId = domRecord[1];
            var topValue = jQuery(this).css("top").replace("px", "");
            var leftValue = jQuery(this).css("left").replace("px", "");
            jQuery("#layer_top_" + domId).val(topValue);
            jQuery("#layer_left_" + domId).val(leftValue);
        }
    });

}