import { defineStore } from "pinia"
import { computed, ref, watch } from "vue"
import { useConfig } from './config'
import { useIntervalFn } from '@vueuse/core'
import { useStatuses } from "./statuses"

export const useCrawler = defineStore('crawler', () => {
    const id = Math.random().toString(36).slice(-5)
    
    const lastSync = ref()
    const runningSince = ref()
    const runningTime = ref()
    const updateRunningTime = () => {
        if (!runningSince.value) return
        runningTime.value = convertMsToTime(Date.now() - runningSince.value)
    }
    const runningInterval = useIntervalFn(updateRunningTime, 1000, { immediate: false })

    const pad = (num: number) => num.toString().padStart(2, '0');
    const convertMsToTime = (milliseconds: number) => {
        let seconds = Math.floor(milliseconds / 1000);
        let minutes = Math.floor(seconds / 60);
        let hours = Math.floor(minutes / 60);
        return `${pad(hours)}:${pad(minutes % 60)}:${pad(seconds % 60)}`;
    }

    const config = useConfig()
    const statuses = useStatuses()
    const crawl = async () => {
        if (!config.data) return
        if (!config.data.lydia) return
        if (!config.data.crawlActive) return
        if (config.data.crawlerId != id) return
        const formData = new FormData();
        formData.append('vendor_token', config.data.lydia.vendorToken)
        formData.append('phone', config.data.lydia.phone)
        formData.append('startDate', config.data.crawlStart)
        const date = new Date()
        const formattedDate = [date.toISOString().slice(0,10), date.toLocaleTimeString()].join(' ')
        formData.append('endDate', formattedDate)
        const response = await fetch('https://lydia-app.com/api/transaction/list.json', {
            method: 'POST',
            body: formData
        })
        const json = await response.json()
        const { transactions } = json
        const newStatuses = transactions.reduce((acc: any, c: any) => {
            const team = c.receiver_description.split(' - ')[0].split(' ')[1].toLowerCase()
            acc[team] += Number(c.amount)
            return acc
        }, { red: 0, blue: 0, green: 0, orange: 0 })
        lastSync.value = formattedDate
        await Promise.all(Object.keys(newStatuses).map((team) => statuses.setStatus(team, { lydia: newStatuses[team] })))
    }

    const crawlInterval = useIntervalFn(crawl, 10000, { immediate: false })
    watch(() => config.data?.crawlActive, (val, oldVal) => {
        if (oldVal && !val) {
            runningInterval.pause()
            crawlInterval.pause()
        } else if (val) {
            runningInterval.resume()
            crawlInterval.resume()
            crawl()
        }
    }, { immediate: true })

    return {
        id,
        lastSync,
        runningSince,
        runningTime
    }
})