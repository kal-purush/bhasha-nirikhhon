import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import * as hanaClient from '@sap/hana-client';
import * as hanaClientPromise from "@sap/hana-client/extension/Promise.js";
import * as hanaOptions from '../../../../default-env.json';

@Injectable()
export class DatabaseService implements OnApplicationShutdown {
  private connection: any;

  constructor() {
    // this.connect();
  }

  private async connectHana() {
    const connParams = {
      serverNode: hanaOptions.host + ':' + hanaOptions.port,
      uid: hanaOptions.user,
      pwd: hanaOptions.password,
      ca: hanaOptions.certificate,
      encrypt: true,
      sslValidateCertificate: true,
      connectTimeout: 3000,
    };

    try {
      this.connection = hanaClient.createConnection();
      await new Promise((resolve, reject) => {
        this.connection.connect(connParams, (err) => {
          if (err) {
            console.error(err);
            reject(err);
          } else {
            console.log('Connected to SAP HANA database sumannnnnnnnnnnnnnnnnnnnnnnnnnnnnnn');
            resolve(null);
          }
        });
      });
    } catch (error) {
      console.error('Error connecting to SAP HANA database:', error);
    }
  }

  isConnected(): boolean {
    return this.connection && this.connection.readyState === 'connected';
  }

  getHanaOptions() {
    return hanaOptions;
  }

  async executeQuery(query: string, connParams : object): Promise<any> {
    return new Promise((resolve, reject) => {
      //   if (this.isConnected()) {
      //     this.connection.exec(query, (err, result) => {
      //       if (err) {
      //         reject(err);
      //       } else {
      //         resolve(result);
      //       }
      //     });
      //   } else {
      //     reject(new Error('Database connection is not available'));
      //   }
      // });
      let conn = hanaClient.createConnection();

      hanaClientPromise.connect(conn, connParams)
        .then(() => {
          return hanaClientPromise.exec(conn, query)
        })
        .then((result) => {
          conn.disconnect()
          resolve(result)
        })
        .catch(err => {
          reject(err)
        })
    });
  }

  async disconnect() {
    if (this.isConnected()) {
      try {
        await this.connection.disconnect();
        console.log('Disconnected from SAP HANA database');
      } catch (error) {
        console.error('Error disconnecting from SAP HANA database:', error);
      }
    }
  }

  onApplicationShutdown() {
    this.disconnect();
  }
}