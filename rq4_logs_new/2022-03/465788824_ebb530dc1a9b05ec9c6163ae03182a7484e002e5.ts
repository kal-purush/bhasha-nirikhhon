
import { DateTime } from 'luxon'

import { config } from "./config"

export class AvailableSlot {
    dt: DateTime
    time: string
    timeZone: string
    href: string

    constructor(data: Partial<AvailableSlot>) {
        Object.assign(this, data)
        if (!this.dt) {
            this.dt = DateTime.fromISO(this.time).setZone(this.timeZone).setLocale("de-DE")
        }
    }

    public isDtInSlot(when: DateTime): boolean {
        const endDt = this.dt.plus({ minutes: config.SLOT_DURATION })
        return this.dt <= when && when <= endDt
    }

    // unused
    public toKV(): Array<string> {
        return [
            this.dt.toISO(),
            this.href
        ]
    }

    public toString(): string {
        return this.dt.toLocaleString(config.DT_LOCALE_STRING_OPTIONS)
    }

}