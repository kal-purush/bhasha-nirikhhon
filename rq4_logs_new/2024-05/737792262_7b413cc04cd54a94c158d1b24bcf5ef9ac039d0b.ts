
// ******************附一：API 示例清单，仅包含一小部分代码  *******************
export {}
 declare global {
  const mockplus: PluginAPI;
   readonly ui: UIAPI;
 
  interface PluginAPI {
    currentPage: Page;  // 当前页面
    readonly root: Document;  // 当前文档
    readonly ui: UIAPI;
    findLayerById(id: string): Layer | null;  // 查找图层 ***
    commitUndo():void; // 提交数据
  }

  interface UIAPI {
    onmessage(message: any): void;  // 接收消息
    postMessage(pluginMessage: any, options?: UIPostMessageOptions);  // 发送消息
  }
  
  interface UIPostMessageOptions {}

  interface Document {
    readonly appID: string;
    readonly pages: ReadonlyArray<Page>;  // 项目中的所有页面
  }
  

//   // 图层结构
//   interface Layer {
//     readonly id: string;
//     removed: boolean;
//     hidden: boolean;  // 是否隐藏
//     locked: boolean;  // 是否锁定
//     readonly layers?: ReadonlyArray<Layer>; // 子图层    
//     remove(): void;
//   }

//   interface Page {    
//     readonly id: string;
//     readonly layers: ReadonlyArray<Layer>;  // 所有子图层 ***
//   }
// }
// ************************* 附二：数据结构声明  ***************************
// interface IMenu {
//   checked: boolean;
//   text: string;
//   id: string; // 唯一
// }

// interface IClearLayerOption {
//   menuList: IMenu[];
//   pageType: EPageType;
// }

// interface IMessage {
//   type: string;
//   data: IClearLayerOption;
// }

// enum EPageType {
//    currentPage,
//    allPage,
// }

// enum EMessage {
//    text,
//    success,
//    fail,
// }