import { Injectable,Module } from '@nestjs/common';
import { ChatGPTAPIBrowser } from "chatgpt";

export class MyGPT {
  
  GPTAPI : ChatGPTAPIBrowser;

  async createAPI(email:string,password:string,nopechaKey?:string): Promise<void> {
    // 자동
    if(nopechaKey){
      const api = new ChatGPTAPIBrowser({ 
        email:email,
        password:password,
        nopechaKey:nopechaKey
      })

      await api.initSession();
      console.log('GPT API 연결 완료 !');

      this.GPTAPI = api;

    }else{
      // 수동
      const api = new ChatGPTAPIBrowser({ 
        email:email,
        password:password,
      })

      await api.initSession();
      console.log('GPT API 연결 완료 !');

      this.GPTAPI = api;

    }
  }
  
  async searchGPT(api:ChatGPTAPIBrowser, str:string) {
    // send a message and wait for the response
    let res = await api.sendMessage(str)
  
    return res.response
  }


}

// export { createAPI, searchGPT };