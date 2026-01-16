import { Client, DdeType } from 'ts-dde';
import { Store as db } from 'ns-store';
import { tryCatch, Util as util, Log } from 'ns-common';

Log.init(Log.category.system, Log.level.ALL);
db.init(require('../../config/config'));
/**
  * @class
  * @classdesc DDE服务器
  */
export class DdeServer {
  // 订阅数据信息
  service: DdeType;
  // 订阅连接对象
  conn: Client;

  constructor(opt: { symbols: string[], items: string[] }) {
    this.service = <DdeType>{ RSS: {} };
    for (const symbol of opt.symbols) {
      this.service.RSS[symbol + '.T'] = opt.items;
    }
    console.log(this.service);

    this.conn = new Client(this.service);
    this.conn.connect();
    if (!this.conn.isConnected()) {
      Log.system.error(`服务:${this.conn.service()},连接失败！`);
    }
    // 断开事件
    this.conn.on('disconnected', () => {
      Log.system.info(`服务:${this.conn.service()},主题:${this.conn.topic()},断开连接！`);
    });
    // 订阅事件
    this.conn.on('advise', (service: string, topic: string, item: string, text: string) => {
      try {
        // 写入数据库
        db.model.Dde.upsert({ service, topic, item, text })
      } catch (e) {
        console.log(e);
      }
    });
    // 订阅DDE数据
    this.conn.startAdvise();
  }

  close() {
    Log.system.info(`关闭dde服务:${this.conn.service()}。`);
    this.conn.stopAdvise();
    this.conn.dispose();
  }
}