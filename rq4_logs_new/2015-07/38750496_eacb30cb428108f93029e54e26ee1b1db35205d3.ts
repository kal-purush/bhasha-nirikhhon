
/// <reference path="typings/tsd.d.ts"/>
/// <reference path="bower_components/controls.ts/controls.ts"/>

module gApp {

    var focus = Controls.makeNoneFocusable("-focus-info");

    var data = [];

    var list: Controls.CListControl;
    list = new Controls.CListControl(null);
    list.setListData(data);
    list.setItemHeight(40);
    list.setAnimation(true);
    list.setScrollScheme(Controls.TParamScrollScheme.EByFixed);
    list.setRedrawAfterOperation(true);
    list.setDataDrawer(function (aKey:any, aItem:any, aEl:HTMLElement) {
        var $item = $(aItem);
        aEl.innerText = $item.find('title').text();
        return Controls.TFocusInfo.KFocusAble;
    });

    var root = new Controls.CLayoutGroupControl(document.body);
    root.setOrientation(Controls.TParamOrientation.EHorizontal);
    root.setOwnedChildControls([list, focus]);
    root.draw();
    root.setActiveFocus();


    document.body.addEventListener('keydown', function (e) {
        var keyStr = e['keyIdentifier'];
        var handled = root.doKey(keyStr);
        console.log(handled);

        var skip = {
            'Up': true,
            'Down': true,
            'Left': true,
            'Right': true
        };

        if (skip[keyStr]) {
            e.stopPropagation();
            e.preventDefault();
        }
    });

    $.get('http://www.smashingmagazine.com/feed/', function(data) {
        var items = $(data).find("item");
        list.setListData(items);
        root.draw();
        root.setActiveFocus();
    });

}