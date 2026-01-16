/*
 *  Script to retrieve pollution data on factories in Taiwan.
 *
 */

// ---------------------- Script Var Setup -----------------
var search_term = '日月光';
var catalog_urls = [];
var factory_urls = [];
var host_url = '';
var fs = require("fs");
var csv_fields = [['申報時段', '廢棄物名稱', '廢棄物清理方式', '產出申報量',	'單位', '申報狀態']];
var timestamp = Math.floor(Date.now() / 1000);
var csv_filename = "/tmp/scrape_file_" + search_term + '_' + timestamp + ".csv";
fs.write(csv_filename, Convert2DArrayToCSV(csv_fields), 'w');
var casper = require("casper").create({
  viewportSize : {width: 1440, height: 400},
  pageSettings: {
    loadImages: false,
    loadPlugins: false,
    userAgent: 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.2 Safari/537.36'
  }
});

casper.on("remote.message", function(message) {
  this.echo("remote console.log: " + message);
});


// ---------------------- Functions to be referenced -----------------

function Convert2DArrayToCSV(objArray) {
    var array = typeof objArray != 'object' ? JSON.parse(objArray) : objArray;
    var str = '';

    for (var i = 0; i < array.length; i++) {
        var line = '';
        for (var index in array[i]) {
            if (line != '') line += ','
            delimiter_escaped_value = array[i][index].replace(/"/g, '""')
            line += '"' + delimiter_escaped_value + '"';
        }

        str += line + '\r\n';
    }

    return str;
}

function parseCatalogPage(destination) {
  destination = typeof destination !== 'undefined' ? destination : '';

  if(destination !== '') {
    casper.thenOpen(destination, function() {

    });
  }

  casper.then ( function () {
    this.echo('Opened location: ' + this.getCurrentUrl());

    factory_urls = []; // clear links

    // Get list of "factory" links to relevant pages.
    var factory_links = this.getElementsAttribute('.data_td a', 'href');

    this.echo("Number of factory pages: " + (factory_links.length));

    for (index = 0; index < factory_links.length; index++) {
      this.echo( 'Factory Page ' + index + ': ' + host_url + factory_links[index]);
      factory_urls.push(host_url + factory_links[index]);


        // Open each "factory" link
        casper.thenOpen(host_url + factory_links[index], function() { // open that link
            this.echo(this.getTitle() + '\n'); // display the title of page

            // Click button to display the portion with the table we need (probably not necessary)
            this.click('#divNavibar_Data a');
        });
        casper.then(function() {
            // Save data to spreadsheet
            this.wait(1000, function() {
                this.capture('/tmp/factory_' + Math.floor(Date.now()) + '.png');
                //var sample = this.getElementsAttribute('#divLatestReport > div:nth-child(3) > table > tbody > tr', 'cells');
                //this.echo(' !-! ' + JSON.stringify(sample));

                // Get table rows
                var tr_data = this.evaluate(function() {
                    //var nodes = document.querySelectorAll('#divLatestReport > div:nth-child(3) > table > tbody > tr');
                    var nodes = document.querySelectorAll('#divLatestReport div.wasHead.textCenter.polHead + table > tbody > tr');
                    var processed_table_rows = Array.prototype.map.call(nodes, function(tr) {
                        //if (tr.children.length != 6) {
                        //    return null; // skip
                        //}
                        return [
                             // Parse all table rows for text and save
                             tr.children[0].innerText.trim(),
                             tr.children[1].innerText.trim(),
                             tr.children[2].innerText.trim(),
                             tr.children[3].innerText.trim(),
                             tr.children[4].innerText.trim(),
                             tr.children[5].innerText.trim(),
                        ];
                    });
                    return processed_table_rows;
                  });


            if( tr_data ) { // make sure tr_data object isnt null
              // Remove the first row, it contains the legend, which should be already in the csv file.
              tr_data.shift();
              var jsonObject = JSON.stringify(tr_data);

              // Convert rows to csv format
              var csv_rows = ( Convert2DArrayToCSV(jsonObject) );
              this.echo(csv_rows);

              // Write CSV rows to file
              fs.write(csv_filename, csv_rows, 'a');
            }
        });
      });

      //break; // enable for testing so that we only sample one factory page per catalog page
    }
  });


}

// ---------------------- Start of Script -----------------
casper.start("http://prtr.epa.gov.tw/FacilityInfo/Data?search=False", function() {

  // get base url of current page -- needs to be added to links as links on pages are relative-pathed.
  var current_url = this.getCurrentUrl();
  var pathArray = current_url.split( '/' );
  var protocol = pathArray[0];
  var host = pathArray[2];
  host_url = protocol + '//' + host;
  this.echo(host_url);

  // fill the search "form" and submit it
  this.sendKeys('input#txtKeyword', search_term);
  this.click('button#btnSearch');

});

casper.waitForSelector( // Wait until the ndivNote div appears, this only appears if results have been retrieved.
  "#divNote",
  function() {
    this.echo('HOST URL ' + host_url);
    this.capture('/tmp/custom-capture_' + Math.floor(Date.now() / 1000) + '.png');
  },
  function() {
    // Results didnt load within time limit - currently 60,000 ms
    this.echo("Search failed");
    this.exit();
  },
  60000);


casper.then(function() {

    // Get count of catalog pages
    var pager_links = this.getElementsAttribute('.pager .pagination a', 'href');
    var last_page_id = 0;
    if(pager_links.length > 2 ) {
        var last_page_id = pager_links.length - 2 ; // Get second to last link
    }
    else if (pager_links.length > 0 ) {
      var last_page_id = pager_links.length - 1 // Will this ever happen?
    }
    this.echo("Last Page ID: " + JSON.stringify((pager_links[last_page_id])));
    this.echo("Number of additional catalog pages: " + (last_page_id +1));

    for (index = 0; index <= last_page_id; index++) {
      this.echo( 'Catalog Page ' + index + ': ' + pager_links[index]);
      catalog_urls.push(host_url + pager_links[index]);
    }

    parseCatalogPage(); // Parse initial catalog page for factory page links, open them, then parse them.

    // Do the same for any additional catalog pages
    for(index = 0; index < catalog_urls.length; index++) {
      parseCatalogPage(catalog_urls[index]);
    }

});


casper.run(function() {
    this.exit();
});