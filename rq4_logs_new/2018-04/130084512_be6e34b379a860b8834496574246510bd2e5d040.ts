export interface TitleResponse {
  response: {
    pages: number;
    results: any[]  };
}

export interface ArticleResponse {
  response: {
    content: {
      webUrl: string;
      blocks: {
        body: any[];
      };
      
    };
  }
  
}