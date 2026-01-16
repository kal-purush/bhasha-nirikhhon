package infinitystorage.api.network;

/**
 * Represents the data handler for channels
 */
public interface IChannelData {

    /**
     * Checks if a machine should connect to the network, and take up a channel
     * @param pipe The pipe in which to check if the machine should connect
     * @return Whether or not the machine should connect to the network, and take up a channel
     */
    boolean shouldConnect(int pipe);

    /**
     * Updates all of the channels, pipes, and machines
     */
    void updateChannels();

    /**
     * Adds or removes however many channels from the network
     * @param channels The amount of channels to add or remove
     * @param aod Add or remove channels; add=true, remove=false
     */
    void changeChannelNumber(int channels, boolean aod);

    /**
     * Gets the amount of channels that have already been used
     * @return The amount of channels that have already been used
     */
    int channelsUsed();

    /**
     * Reloads all of the data concerning the cables connected to the controller
     */
    void reloadCables();
}