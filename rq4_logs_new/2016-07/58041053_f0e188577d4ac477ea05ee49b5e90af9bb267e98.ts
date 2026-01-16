module OPENSets.Services {
  export class StatisticsService {
    private static _instance: StatisticsService = new StatisticsService();

    private gameName: string = 'Sets';
    private guid: string;

    public static getInstance(): StatisticsService {
      return StatisticsService._instance;
    }

    constructor() {
      if (StatisticsService._instance) {
        throw new Error('Error: Instantiation failed: Use StatisticsService.getInstance() instead of new.');
      }

      StatisticsService._instance = this;
    }

    createXMLHttpRequest(): XMLHttpRequest {
      return new XMLHttpRequest();
    }

    startGame(): void {
      let http: XMLHttpRequest = this.createXMLHttpRequest();
      let that: StatisticsService = this;
      http.onreadystatechange = () => {
        if (http.readyState === 4 && http.status === 200) {
          console.log('Start game response - GUID: ' + http.responseText);
          that.setGuid(http.responseText);
        }
      };

      http.open('GET', 'http://localhost:3000/api/gameStarted/:' + this.gameName, true);
      http.send();
    }

    endGame(): void {
      let http: XMLHttpRequest = this.createXMLHttpRequest();
      let that: StatisticsService = this;
      http.onreadystatechange = () => {
        if (http.readyState === 4 && http.status === 200) {
          console.log('End game response: ' + http.responseText);
          that.setGuid(undefined);
        }
      };
      http.open('GET', 'http://localhost:3000/api/gameEnded/' + this.guid, true);
      http.send();
    }

    updateGame(misses: number): void {
      let http: XMLHttpRequest = this.createXMLHttpRequest();
      http.onreadystatechange = () => {
        if (http.readyState === 4 && http.status === 200) {
          console.log('Update game response: ' + http.responseText);
        }
      };
      http.open('GET', 'http://localhost:3000/api/gameUpdate/?guid=' + this.guid + '&misses=' + misses, true);
      http.send();
    }

    setGuid(guid: string): void {
      this.guid = guid;
    }
  }
}