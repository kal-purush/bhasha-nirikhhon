import { GoogleRequest } from "./google.request";

export class GoogleData {

    private _request: any = null;
    
    private _subList: any[] = [];

    private _timeLimit: Date = new Date();

    constructor() {
    }

    init(api:any):Promise<any> {
    
        this._request = new GoogleRequest(api);

        return new Promise((resolve,reject) => {

            this._request.requestUserInfo().then( (user:any) => {
                
                if(!localStorage.getItem('uid')) localStorage.setItem('uid',user.id);
                if (localStorage.getItem('uid') !== user.id) {

                    localStorage.setItem('fv', (Date.now() - 86400000).toString())
                    localStorage.setItem('uid', user.id)

                }

                if (!localStorage.getItem('fv')) localStorage.setItem('fv', (Date.now() - 86400000).toString());

                this._timeLimit.setTime(parseInt(<string>localStorage.getItem('fv')))

                resolve();

            });

        })

    }

    dispose() {
        this._request = null;
        this._subList = [];
    }

    getSubList():Promise<any[]> {

        return new Promise( (resolve, reject) => {

            if (this._subList.length) {
                resolve(this._subList);
                console.log("sublist already exists");
                return;
            }

            this._request.requestSubscriptions().then( (list:any[]) => {

                this._subList = list.map((channel) => {

                    const channelInfo: any = channel.snippet;

                    return {
                        id: channelInfo.resourceId.channelId,
                        title: channelInfo.title,
                        // descirption: channelInfo.descirption,
                        thumbnail: channelInfo.thumbnails.default.url,
                        uploadsId: channelInfo.resourceId.channelId.replace(/UC/,"UU")
                    }

                })

                // this.getChannelsUploadsId()
                resolve(this._subList);
            })
        });

    }

    getAllVideos():Promise<any[]> {

        return new Promise((resolve, reject) => {

            this.getSubList().then(() => {

                const promises:any[] = [];

                this._subList.map( (sub:any) => {
                    promises.push( this.getChannelVideos(sub) );
                })

            
                Promise.all(promises).then( () => {
                    resolve(this.makeVideoList());
                }) 

            });
        }) 

    }

    getVideosByDate(order: number) {

        let orderNum = 1;

        if (order == 1) orderNum = -1;

        return this.makeVideoList().sort((a, b) => {
            const dateA: Date = new Date(a.date);
            const dateB: Date = new Date(b.date);
            return orderNum * (dateB.getTime() - dateA.getTime());
        })
    }

    private makeVideoList():any[] {
        const list = this._subList.map((sub) => sub.videos);

        let result:any[] = [];

        for(let videoList of list) {
            result = [...result,...videoList]
        }

        return result;
    }

    private getChannelVideos(id:any,nextPage?:any):Promise<any[]> {

        return new Promise( (resolve, reject) => {

            let channel: any = id.id ? id : null;

            if(!channel) {
                this._subList.map((sub) => {
                    if (id == sub.id) {
                        channel = sub;
                    }
                })
            }

            this._request.requestUploads(channel.uploadsId,nextPage).then((data:any) => {

                if (!channel.videos) channel.videos = [];

                let videos = data.items;
                const token = data.token;

                let moreVideos = true;

                videos = videos.filter( (video:any) => {

                    if (Date.parse(video.snippet.publishedAt) < this._timeLimit.getTime()) moreVideos = false;

                    return Date.parse(video.snippet.publishedAt) >= this._timeLimit.getTime();

                })

                videos.map( (video:any) => {

                    const videoId = video.snippet.resourceId.videoId;

                    for( let video of channel.videos) {
                        if(videoId == video.id) return
                    }

                    channel.videos.push({
                        id: videoId,
                        thumbnail: video.snippet.thumbnails.medium,
                        title: video.snippet.title,
                        author: channel.title,
                        date: video.snippet.publishedAt
                    });

                })

                if (moreVideos) { 
                    this.getChannelVideos(channel,token).then( () => resolve() );
                    return
                }

                resolve();

            })

        })

    }

    //this isn't useful since youtube uses UC for channel ids and UU for their upload playlist ?
    private getChannelsUploadsId(): Promise<string> {

        return new Promise((resolve, reject) => {

            const ids: string[] = this._subList.map((sub) => sub.id);

            const promises: any[] = [];

            const amountOfRequests = ids.length % 50 > 0 ? (ids.length - (ids.length % 50)) / 50 + 1 : ids.length / 50;

            for (let i = 1; i <= amountOfRequests; i++) {

                const idsString: string = ids.splice(0, 50).toString();
                promises.push(this._request.requestChannel(idsString));

            }

            Promise.all(promises).then((channels: any[]) => {
                for (let channelsList of channels) {
                    channelsList.map((channel: any) => {
                        const uploadsId: string = channel.contentDetails.relatedPlaylists.uploads;

                        this._subList.map((sub) => {
                            if (channel.id === sub.id) {
                                sub.uploadsId = uploadsId;
                            }
                        })
                    })
                }
                resolve()
            });
        })

    }
}