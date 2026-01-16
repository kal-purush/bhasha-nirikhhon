export class Appeal{
  constructor(
    public username: string, // id
    public pillar: string,
    public date: string, 
    public title?: string,
    public content?: string,
    ){}
  
}