///<reference path='../interfaces/IDataChannel.ts'/>
///<reference path='../interfaces/IDataStorageConfiguration.ts'/>

export class DataStorage {

    private configuration: IDataStorageConfiguration;
    private channelCreator: (configuration: IDataChannelConfiguration) => IDataChannel;
    private channels: { [name: string] : IDataChannel };

    constructor(configuration: IDataStorageConfiguration, dataChannelCreator: (configuration: IDataChannelConfiguration) => IDataChannel) {
        this.configuration = configuration;
        this.channelCreator = dataChannelCreator;
        this.channels = {};
    }

    getChannel(name: string) : IDataChannel {
        if (this.channels.hasOwnProperty(name)) {
            return this.channels[name];
        }
        var channelConfiguration = this.configuration.channels[name];
        if (!channelConfiguration) {
            return null;
        }
        var dataChannel = this.createChannel(channelConfiguration);
        this.channels[name] = dataChannel;
        return dataChannel;
    }

    private createChannel(configuration: IDataChannelConfiguration): IDataChannel {
        return this.channelCreator(configuration);
    }
}