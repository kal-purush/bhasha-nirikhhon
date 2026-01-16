
export class Config {
    private static instance:Config;

    private data = {
        algoliaIndex: "your-index",
        algoliaApp: "your-algolia-app-id",
        algoliaKey: "your-algolia-access-key",
    };

    public static getInstance():Config {
        if(Config.instance == null) {
            Config.instance = new Config();
        }
        return Config.instance;
    }

    public get(key){
        if(this.data.hasOwnProperty(key)){
            return this.data[key];
        }
        return null;
    }

    public static get(key){
        let config = Config.getInstance();

        return config.get(key);
    }
}