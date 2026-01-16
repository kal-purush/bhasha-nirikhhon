import { request } from "./utils"

export interface Event<T> {
    id: string
    name: string
    eventId: string
    data: T
    blockchain: string
    projectId: string
    project?: any
    iamUserId: string
    iamUser?: any
    createdAt: Date
}

export interface IamUser {
    id?: string
    address: string
    projectId: string
    blockchains: string
    project: any
    events: Event<any>[]
    transactionVolume: number
    transactionExecuted: number
}

export interface WalletConnectedEvent extends Event<{address: string, id?: string, startTime: Date, endTime: Date}> {}
export interface PageViewedEvent extends Event<{url: string, referrer: string, time: Date}> {}

export const findEventsByProject = async (projectId: string): Promise<Event<any>[]> => {
    try {
        const data = await request({
            url: "/events",
            data: {
                project: projectId
            },
            method: 'GET'
        })

        console.log(data.data, "1234", projectId)
        return data.data
    } catch (e: any) {
        console.log(e.response.data, "1234", projectId)
        throw new Error(e.response.data)
    }
}

export const findIamByProject = async (projectId: string): Promise<IamUser[]> => {
    try {
        console.log(projectId, "12345")
        const data = await request({
            url: "/iam",
            data: {
                project: projectId
            },
            method: 'GET'
        })

        return data.data
    } catch (e: any) {
        throw new Error(e.response.data)
    }
}

export const convertWalletConnectedGraph = (events: WalletConnectedEvent[]) => {
    try {
        let seenAddresses: { [key: string]: { seen: boolean, week: Date } } = {}
        let weeklyData: { [key: string]: { startDate: Date, endDate: Date, number: number } } = {}

        let today = new Date()
        today.setDate(today.getDate() - (today.getDay() + 6) % 7)
        today.setHours(12)
        today.setMinutes(12)
        today.setSeconds(12)

        for(let i = 0; i < 8; i++) {
            weeklyData[today.toDateString()] = {
                startDate: new Date(today.toISOString()),
                endDate: new Date(today.getFullYear(), today.getMonth(), today.getDate() + 6),
                number: 0
            }

            today.setDate(today.getDate() - 7)
        }

        for(let i = 0; i < events.length; i++) {
            let thisWeek = events[i].data.startTime
            thisWeek.setDate(thisWeek.getDate() - (thisWeek.getDay() + 6) % 7)
            
            if(seenAddresses[events[i].data.address] && seenAddresses[events[i].data.address].seen && seenAddresses[events[i].data.address].week.getTime() === thisWeek.getTime()) continue
            
            console.log(thisWeek.toDateString(), events[i].data.startTime)
            weeklyData[thisWeek.toDateString()] = {
                ...weeklyData[thisWeek.toDateString()],
                number: weeklyData[thisWeek.toDateString()].number + 1
            }

            seenAddresses[events[i].data.address] = {
                seen: true,
                week: thisWeek
            }
        }

        console.log(weeklyData, "weeklydata")
        return weeklyData
    } catch (e: any) {
        throw e
    }
}

export const convertTransactionVolumeGraph = (events: Event<{ hash: string, time: Date, volume: number }>[]) => {
    try {
        let seenAddresses: { [key: string]: boolean } = {}
        let weeklyData: { [key: string]: { startDate: Date, endDate: Date, number: number } } = {}

        let today = new Date()
        today.setDate(today.getDate() - (today.getDay() + 6) % 7)
        today.setHours(12)
        today.setMinutes(12)
        today.setSeconds(12)

        for(let i = 0; i < 8; i++) {
            weeklyData[today.toDateString()] = {
                startDate: today,
                endDate: new Date(today.getFullYear(), today.getMonth(), today.getDate() + 6),
                number: 0
            }

            today.setDate(today.getDate() - 6)
        }

        for(let i = 0; i < events.length; i++) {
            let thisWeek = new Date()
            thisWeek.setDate(thisWeek.getDate() - (thisWeek.getDay() + 6) % 7)

            if(seenAddresses[events[i].data.hash]) continue

            weeklyData[thisWeek.toDateString()] = {
                ...weeklyData[thisWeek.toDateString()],
                number: weeklyData[thisWeek.toDateString()].number + events[i].data.volume
            }

            seenAddresses[events[i].data.hash] = true
        }

        return weeklyData
    } catch (e: any) {
        throw e
    }
}

export const generateRetentionTime = (events: Event<{address: string, id?: string, startTime: Date, endTime: Date}>[]) => {
    try {
        let seenAddresses: { [key: string]: boolean } = {}
        let weeklyData: { [key: string]: { startDate: Date, endDate: Date, number: number, retention: any[] } } = {}

        let today = new Date()
        today.setDate(today.getDate() - (today.getDay() + 6) % 7)
        today.setHours(12)
        today.setMinutes(12)
        today.setSeconds(12)

        for(let i = 0; i < 8; i++) {
            weeklyData[today.toDateString()] = {
                startDate: today,
                endDate: new Date(today.getFullYear(), today.getMonth(), today.getDate() + 6),
                number: 0,
                retention: []
            }

            today.setDate(today.getDate() - 7)
        }

        for(let i = 0; i < events.length; i++) {
            let endTime = new Date()
            if('endTime' in events[i].data) endTime = new Date(events[i].data.endTime)
            console.log(endTime, "satyam")

            const retentionTime = (endTime.getTime() - new Date(events[i].data.startTime).getTime()) / 1000
            let thisWeek = new Date(events[i].data.startTime)
            thisWeek.setDate(thisWeek.getDate() - (thisWeek.getDay() + 6) % 7)

            weeklyData[thisWeek.toDateString()].retention.push({
                startTime: new Date(events[i].data.startTime),
                endTime: endTime,
                retentionTimeInSec: retentionTime
            })

            weeklyData[thisWeek.toDateString()].number = (weeklyData[thisWeek.toDateString()].number + retentionTime) / weeklyData[thisWeek.toDateString()].retention.length
        }

        console.log(weeklyData)

        return weeklyData
    } catch (e: any) {
        throw e
    }
}

export const generatePageViewsGraph = (events: Event<{ pageUrl: string, referrer: string }>[]) => {
    try {
        let weeklyData: { [key: string]: { startDate: Date, endDate: Date, number: number } } = {}

        let today = new Date()
        today.setDate(today.getDate() - (today.getDay() + 6) % 7)
        today.setHours(12)
        today.setMinutes(12)
        today.setSeconds(12)

        for(let i = 0; i < 8; i++) {
            weeklyData[today.toDateString()] = {
                startDate: today,
                endDate: new Date(today.getFullYear(), today.getMonth(), today.getDate() + 6),
                number: 0,
            }

            today.setDate(today.getDate() - 7)
        }

        for(let i = 0; i < events.length; i++) {
            let thisWeek = events[i].createdAt
            thisWeek.setDate(thisWeek.getDate() - (thisWeek.getDay() + 6) % 7)

            weeklyData[thisWeek.toDateString()].number = weeklyData[thisWeek.toDateString()].number + 1
        }

        console.log(weeklyData)

        return weeklyData
    } catch (e: any) {
        throw e
    }
}