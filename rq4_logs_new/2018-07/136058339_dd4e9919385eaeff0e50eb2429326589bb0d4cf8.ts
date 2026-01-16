import { Facenet } from '../'
import { unlink, writeFile } from "fs";
import { IncomingMessage, ServerResponse } from "http";

export class FacenetService {
  private facenet: Facenet
  private isReady: Boolean

  constructor(callback) {
    this.isReady = false;
    this.facenet = new Facenet();
    this.warmUp().then(() => {
      if(callback) {
        callback();
      }
    });
  }

  private async warmUp() : Promise<void> {
    try {
      // Load image from file
      const imageFile = `${__dirname}/../tests/fixtures/two-faces.jpg`
      // Do Face Alignment, return faces
      let faceList = await this.facenet.align(imageFile);
      await this.facenet.embedding(faceList[0]);
      await this.facenet.embedding(faceList[1]);
    } finally {
      this.isReady = true;
      console.log('Facenet started');
    }
  }

  public async extractEmbeddingsFromFile(fileName: String): void {
    this.facenet.align(fileName).then((faceList) => {
      this.buildEmbbedings(faceList).then(() => {
        return await faceList.map(item => ({
          location: item.location,
          landmark: item.landmark,
          embedding: item.embedding ? item.embedding.tolist() : []
        }))
      });
    });
  }

  public serveApi(request: IncomingMessage, response: ServerResponse): void {
    response.setHeader('Content-Type', 'application/json');
    response.writeHead(200);
    if (this.isReady) {
      var payload = "";
      request.on('data', (data) => {
        payload += data;
      });
      request.on('end', () => {
        const data = JSON.parse(payload);
        if (data.command === 1) {
          let base64Image = payload.split(';base64,').pop();

          const newFileName = (new Date()).getTime().toString() + '-' + Math.round(Math.random() * 100000) + '.jpg';

          writeFile(newFileName, base64Image, {encoding: 'base64'}, (err) => {
            if (err) {
              response.write('{}');
              response.end();
              return;
            }

            this.extractEmbeddingsFromFile().then((result)=>{
              response.write(JSON.stringify(result));
              response.end();
              unlink(newFileName, () => {
              });
            })
          });
        } else {
          response.write('{}');
          response.end();
        }

      });
    } else {
      response.write('{}');
      response.end();
    }
  }

  private async buildEmbbedings(faceList): Promise<void> {
    for (var i = 0; i < faceList.length; i++) {
      var emb = await this.facenet.embedding(faceList[i])
      faceList[i].embedding = emb; //.tolist();
    }
  }
}