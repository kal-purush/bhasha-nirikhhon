export class MockSuites {

  testSuites: Array<Object> = [];
  limit: number = 10;

  /**
   * @Name: constructor
   * @Description:
   */


  constructor() {
    // Generate the suites and their builds
    for (var i = 0; i < this.limit; i++) {
      var suite = {
        id: i,
        suiteName: 'Suite ' + (i+1),
        builds: this.mockBuilds()
      };
      this.testSuites.push(suite);
    }
  }


  get(): Object[] {
    return this.testSuites;
  }


  mockBuilds() {
    var mockBuildData = [];
    for (var i = 0; i < 14; i++) {
      var mockDate = new Date('10-'+(31-i)+'-2015'),
        mockBuild = {
          id: i,
          date: mockDate,
          status: 'pass',
          totalSpecs: 0,
          totalPasses: Math.round(Math.random()*10),
          totalFails: Math.round(Math.random()*10),
          totalSkips: Math.round(Math.random()*10),
          url: 'http://www.jenkins.com'
        };
      mockBuild.totalSpecs = mockBuild.totalPasses + mockBuild.totalFails + mockBuild.totalSkips;
      if (mockBuild.totalFails > 0) {
        mockBuild.status = 'fail';
      }
      mockBuildData.push(mockBuild);
    }
    return mockBuildData;
  }







}