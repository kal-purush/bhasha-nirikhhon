var TogglClient = require('toggl-api');
var toggl = new TogglClient({apiToken: '8264f06173878d57440c7a22d6ced2d0'});

/*console.log(err);

/*8264f06173878d57440c7a22d6ced2d0*/

toggl.summaryReport(
    {
    workspace_id : 1497556,
    user_agent : 'Bardo',
    since: '2016-08-02',
    until : '2016-08-02' },
    function (err, report) {        
        report.data.forEach(function(element) {
        console.log(element.title.project)
        var date = new Date(element.time);
        var h = Math.floor(element.time / 3600000);
        var m = date.getMinutes();
        var s = date.getSeconds();
        console.log( h + ":" + m + ":" + s ) ;    
        }, this);
        
        
    }
);

/*toggl.getTimeEntries(
    startDate = '2016-08-01T00:00:01+02:00',
    endDate = '2016-08-02T00:00:01+02:00',
 function (err, timeEntries) {
    timeEntries.forEach(function(element) {
        toggl.getProjectData(
            projectId = element.pid, function (err, projecData) {
                console.log(projecData.name)
                console.log(element.duration)        
            }
        )
        
    }, this);
});
    //{
   // start_date: '2016-07-01T00:00:01+02:00',
  //  end_date: '2016-07-02T00:00:01+02:00'
//}, function (err, timeEntries) {});
   //timeEntries.forEach(function(element) {
     //  console.log(element.id)
 //  } ); 
//});

/*
toggl.startTimeEntry({
  description: 'Some cool work',
  billable:    false
}, function(err, timeEntry) {
  // handle error

  // working 10 seconds
  setTimeout(function() {
    toggl.stopTimeEntry(timeEntry.id, function(err) {
      // handle error
      console.log(JSON.stringify(timeEntry.id));

      toggl.updateTimeEntry(timeEntry.id, {tags: ['finished']}, function(err) {
        toggl.destroy()
      });
    });
  }, 120);
}); */