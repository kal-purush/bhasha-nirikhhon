jQuery(function () {
    jQuery.post(
        DOKU_BASE + 'lib/exe/ajax.php',
        {call: 'epnsmartlink_getconf'}
    ).success(function (data) {
        if (data['campaign']) {
            window._epn = {campaign: data['campaign']};
            jQuery.getScript(data['url']);
        }
    });
});