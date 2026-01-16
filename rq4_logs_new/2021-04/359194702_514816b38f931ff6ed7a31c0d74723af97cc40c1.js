/*
See http://kvdb.net/projects/6pp on how to use this script.
*/

// Namespacing the code so the examples don't clash.
var net = {};
net.kvdb = {};
net.kvdb.Lookup = {};
var _lookup = net.kvdb.Lookup;

_lookup.fromPostcode = function(e) {
    // Only request a lookup if the user pressed enter from the postcode field.
    // Otherwise, wait for street number to be entered.
    var keynum;
    if(window.event) // IE
    {
        keynum = window.event.keyCode;
    }
    else if(e.which) // Netscape/Firefox/Opera
    {
        keynum = e.which;
    }

    var KEY_TAB = 9;
    var KEY_RETURN = 13;
    if (keynum == KEY_RETURN || keynum == KEY_TAB)
    {
        this.lookup();
    }
}

_lookup.lookup = function() {
    // Perform an address lookup based on postcode and -if entered- street number.

    // Clear the street / city result fields.
    this.resetResults();

    // Enable spinner, so the user knows the lookup is being performed.
    var spinner = document.getElementById("spinner");
    if (spinner) {
        spinner.style.display = '';
    }

    var streetnumber = document.getElementById('streetnumber').value;
    var postcode = document.getElementById('postcode').value;
    this.loadScript('http://6pp.kvdb.net/services/lookup?postcode=' + escape(postcode) + '&streetnumber=' + escape(streetnumber) + '&tg_format=json&jsonp=_lookup.showLookupResults');
}

_lookup.edit = function() {
    // Return street information with postcode + street number to improve the 6pp database.
        var postcode = document.getElementById('postcode').value;
    var streets = document.getElementById('streets');
        // But only if the user chose a street instead of the default text.
    if (streets.selectedIndex == 0) { return; }
    var straatnaam = streets.options[streets.selectedIndex].text;
    var city = document.getElementById('city').value;
    var streetnumber = document.getElementById('streetnumber').value;
    // After the submit, perform lookup again to get the new data.
    this.loadScript('http://6pp.kvdb.net/services/feedback?postcode=' + escape(postcode) + '&street=' + escape(straatnaam) + '&city=' + escape(city) + '&streetnumber=' + escape(streetnumber) + '&tg_format=json&jsonp=lookup');
}

_lookup.loadScript = function(url) {
    var script = document.createElement('script');
    script.src = url;
    document.getElementsByTagName('head')[0].appendChild(script);
}

_lookup.resetResults = function() {
    // Empty single street result field
    document.getElementById('street').value = "";
    // Empty multiple streets result listbox
    document.getElementById('streets').options.lenght = 0;
    // Empty city result field
    document.getElementById('city').value = "";
    // Hide the map
    document.getElementById('map').style.display = "none";
        // Hide the unsure link
        document.getElementById('unsure').style.display = "none";
        // Hide the fix link
        document.getElementById('fix').style.display = "none";
        // Hide the add link
        document.getElementById('add').style.display = "none";
    // Hide the addanother link
    document.getElementById('addanother').style.display = "none";
}

_lookup.showSingleAddress = function() {
    // Hide the multiple address elements
    var streets = document.getElementById('streets');
    streets.style.display = 'none';
    // Show the single address elements
    var street = document.getElementById('street');
    street.style.display = '';
}

_lookup.showManyAddresses = function() {
    // Hide the single address elements
    var street = document.getElementById('street');
    street.style.display = 'none';
    // Show the multiple address elements
    var streets = document.getElementById('streets');
    streets.style.display = '';
}

_lookup.showLookupResults = function(json) {
    // Disable spinner
    var spinner = document.getElementById("spinner");
    if (spinner) {
        spinner.style.display = 'none';
    }

        if (json.result == 'Invalid input') {
            document.getElementById('city').value = "Ongeldige invoer";
            return;
        }
        var count = json.result.length;
    if (count == 0) {
        // No results found
    } else {
        // Always display the city
        document.getElementById('city').value = json.result[0].city;
        if (count == 1) {
            // There's exactly one address available.
            this.showSingleAddress();
            var street = json.result[0].street;
            var subtitle = json.result[0].subtitle;
            var street_id = json.result[0].street_id;
            var postcode_id = json.result[0].postcode_id;
            var postcode = document.getElementById('postcode').value;
            var chars = postcode.substr(4).replace(/^\s+/,'').replace(/\s+$/,'').toUpperCase();

            // On 6PP lookups, allow user to add unknown streets.
            if (!street && postcode.length >= 6) {
                street = "Straat onbekend";
                var add = document.getElementById('add');
                add.href = "http://6pp.kvdb.net/wiki/add?street=&postcode_id=" + postcode_id + "&chars=" + chars;
                add.style.display = "inline";
            }
            document.getElementById('street').value = street;
            document.getElementById('street').title = subtitle;
            if (street_id) {
                // Allow user to flag the street as possibly invalid (unsure).
                var unsure = document.getElementById('unsure');
                unsure.onclick = new Function("_lookup.unsureStreet(" + street_id +"); return false;");
                unsure.style.display = "inline";
                // Allow user to add an extra street in addition to the one(s) seen.
                var addanother = document.getElementById('addanother');
                addanother.href = "http://6pp.kvdb.net/wiki/add?street=&postcode_id=" + postcode_id + "&chars=" + chars;    
                addanother.style.display = "inline";
            }
        } else {
            // There are more known addresses for this postcode (+ street number). Show all options.
            this.showManyAddresses();
            var streets = document.getElementById('streets');
            streets.options[0] = new Option("Maak uw keuze uit " + count + " straten.", "Placeholder");
            for (var i = 0; i < count; i++)
            {
                var street = json.result[i].street;
                streets.options[i+1] = new Option(street, i);
            }
        }
        // Show geo-coordinates on a map, if available.
        var lat = json.result[0].geo_lat;
        var lng = json.result[0].geo_long;
        if (lat && lng) {
            var map = document.getElementById('map');
            // Map was not visible, show it now.
            map.style.display = "block";
            map.src = 'examples/kaart.php?size=small&lat=' + lat + '&long=' + lng;
        }
    }
}

_lookup.unsureStreet = function(street_id) {
    // User chose to flag this street as unsure.
    
    // Enable spinner, so the user knows the action is being performed.
    var spinner = document.getElementById("spinner");
    if (spinner) {
        spinner.style.display = '';
    }

    this.loadScript('http://6pp.kvdb.net/services/setstreet?id=' + street_id + '&unsure=True' + '&tg_format=json&jsonp=_lookup.unsureResults');

    // The unsure link is no longer required.
    var unsure = document.getElementById('unsure');
    unsure.style.display = "none";
}

_lookup.unsureResults = function(json) {
    // The street has been flagged unsure.

    // Disable spinner
    var spinner = document.getElementById("spinner");
    if (spinner) {
        spinner.style.display = 'none';
    }

    // Hope the user will fix the spotted error too.
    var fix = document.getElementById('fix');
    fix.href = "http://6pp.kvdb.net/wiki/edit?street_id=" + json.result.id;    
    fix.style.display = "inline";
}