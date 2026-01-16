import { ServerStatus } from "../enums"
import { getPlayerCount, isServerOnline, stopServer } from "../service/apiService"
import { ActivityType, type Client } from 'discord.js'

let LAST_CHECKED_PLAYERS_ONLINE = true

export default async function checkPlayersOnline(discordClient : Client<boolean>) : Promise<void> {
    isServerOnline().then(async isOnline => {
        if(isOnline) {
            const currentlyHasPlayersOnline = await getPlayerCount() > 0
            if(!LAST_CHECKED_PLAYERS_ONLINE && !currentlyHasPlayersOnline) {
                console.log('Automatically stopping server')
                discordClient.user?.setActivity({
                    name: ServerStatus.OFFLINE,
                    type: ActivityType.Custom
                })
                stopServer()
            } else {
                LAST_CHECKED_PLAYERS_ONLINE = currentlyHasPlayersOnline
            }
        }
    })
}