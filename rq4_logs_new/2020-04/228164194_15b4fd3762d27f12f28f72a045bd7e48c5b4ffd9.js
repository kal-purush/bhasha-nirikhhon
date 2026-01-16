//
//  Handler script for the Elite: Dangerous information panel within
//  the Custom ClangNet part of the web interface.
//

// Function that queries all of the data that is required.
$(run = function () {
    // Get the settings for EDInfo.
    socket.getDBValues('edinfo_get_settings', {
        tables: ['modules'],
        keys: ['./custom/edinfo.js']
    }, true, function (e) {
            if (!helpers.getModuleStatus('edinfoModule', e['./custom/edinfo.js'], 'edinfoModuleToggle')) {
                return;
            }
    });
});

$(function () {
    // Allow for the toggle on/off of the module.
    $('#edinfoModuleToggle').on('change', function () {
        socket.sendCommandSync('edinfo_module_toggle_cmd', 'module ' + ($(this).is(':checked') ? 'enablesilent' : 'disablesilent') + './custom/edinfo.js', run);
    });

    socket.getDBTableValues('get_all_builds', 'edShipBuild', function (results) {
        let builds = [];

        for (let i = 0; i < results.length; i++) {
            builds.push([
                (i + 1),
                results[i].key,
                results[i].value
            ]);
        }

        // If the table exists, destroy it to start afresh.
        if ($.fn.DataTable.isDataTable('#shipBuildTable')) {
            $('#shipBuildTable').DataTable().destroy();
            // Remove all of the old events.
            $('#shipBuildTable').off();
        }

        // Create the table.
        $('#shipBuildTable').DataTable({
            'searching': true,
            'autoWidth': false,
            'data': builds,
            'columnDefs': [
                { 'width': '30%', 'targets': 0 }
            ],
            'columns': [
                { 'title': 'Entry No.' },
                { 'title': 'Ship Name' },
                { 'title': 'Build URL' }
            ]
        });
    });

    // Settings button.
    $('#edinfo-settings-button').on('click', function () {
        socket.getDBValues('get_edinfo_settings', {
            tables: ['edInfo'],
            keys: ['allowOffline']
        }, true, function (e) {
            helpers.getModal('edinfo-settings', 'Elite: Dangerous Info Settings', 'Save', $('<form/>', {
                'role': 'form'
            })
                // Append a select option for the toggle.
                .append(helpers.getDropdownGroup('offline-toggle', 'Enable Offline Usage',
                    (e.allowOffline === 'true' ? 'Yes' : 'No'), ['Yes', 'No'])),
                function () { // Callback for when the user clicks save.
                    let offlineToggle = $('#offline-toggle').find(':selected').text() === 'Yes';

                    switch (false) {
                        default:
                            socket.updateDBValues('update_edinfo_settings', {
                                tables: ['edInfo'],
                                keys: ['allowOffline'],
                                values: [offlineToggle]
                            }, function () {
                                socket.sendCommand('update_edinfo_settings_cmd', 'reloadedinfo', function () {
                                    // Close the modal.
                                    $('#edinfo-settings').modal('toggle');
                                    // Alert the user.
                                    toastr.success('Successfully updated offline usage settings!');
                                });
                            });
                    }
                }).modal('toggle');
        });
    });
});