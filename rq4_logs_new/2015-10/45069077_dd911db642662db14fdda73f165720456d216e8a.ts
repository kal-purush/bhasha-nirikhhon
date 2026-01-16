import { Component, NgFor, NgIf } from 'angular2/angular2';
//import { moment } from 'moment';

@Component({
  selector: 'home',
  templateUrl: './components/home/home.html',
  styleUrls: ['./components/home/home.css'],
  directives: [NgFor, NgIf]
})
export class HomeCmp {
  testSuites: Array<Object>;
  months: Array<string>;

  constructor () {
    this.testSuites = [
      {
        id: 1,
        suiteName: "Suite 1",
        builds: this.mockBuilds()
      },
      {
        id: 2,
        suiteName: "Suite 2",
        builds: this.mockBuilds()
      },
      {
        id: 3,
        suiteName: "Suite 3",
        builds: this.mockBuilds()
      },
      {
        id: 4,
        suiteName: "Suite 4",
        builds: this.mockBuilds()
      },
      {
        id: 5,
        suiteName: "Suite 5",
        builds: this.mockBuilds()
      },
      {
        id: 6,
        suiteName: "Suite 6",
        builds: this.mockBuilds()
      },
      {
        id: 7,
        suiteName: "Suite 7",
        builds: this.mockBuilds()
      },
      {
        id: 8,
        suiteName: "Suite 8",
        builds: this.mockBuilds()
      }
    ];
    this.months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  }


  formatTime(timestamp: string) {
    var date = new Date(timestamp),
        day = date.getDate(),
        monthIndex = date.getMonth(),
        year = date.getFullYear();



    return day + '-' + this.months[monthIndex];
  }

  // Mock Utils
  // TODO: delete all of these
  mockBuilds() {
    var mockBuildData = [];
    for (var i = 0; i < 14; i++) {
      var mockDate = new Date('10-'+(30-i)+'-2015'),
          mockBuild = {
            id: i,
            date: mockDate,
            status: "pass",
            totalSpecs: 0,
            totalPasses: Math.round(Math.random()*10),
            totalFails: Math.round(Math.random()*10),
            totalSkips: Math.round(Math.random()*10),
            url: "http://www.jenkins.com"
          };
          mockBuild.totalSpecs = mockBuild.totalPasses + mockBuild.totalFails + mockBuild.totalSkips;
          if(mockBuild.totalFails > 0){
            mockBuild.status = "fail";
          }
          mockBuildData.push(mockBuild);
    }
    return mockBuildData;
  }

}