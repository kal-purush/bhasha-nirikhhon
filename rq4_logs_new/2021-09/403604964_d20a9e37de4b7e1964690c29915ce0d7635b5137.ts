export interface LogHeaders {
    Version: string
    Fields: string[]
    Software?: string
    "Start-Date"?: string
    "End-Date"?: string
    Date?: string
    Remark?: string
}

export default class ExtendedLogParser {
    private static headers: string[] = ["Version", "Fields", "Software", "Start-Date", "End-Date", "Date", "Remark"]

    /**
     * Extended Log Parser
     * @param log - log string
     * @param delimiter - field delimiter @default=whitespace
     */
    static parse(log: string, delimiter: string = " ") {
        const logs: object[] = []
        const lines: string[] = log.split('\n')

        const headers = this.getHeaders(lines)

        for (const line of lines) {
            if (!line || line.startsWith('#') || line === '') continue
            logs.push(this.lineToObject(line, headers.Fields, delimiter))
        }

        return logs

    }

    private static getHeaders(lines: string[]): LogHeaders {
        const headers: LogHeaders = {
            Version: "1.0",
            Fields: []
        }
        for (const line of lines) {
            if (!line.startsWith("#")) {
                break
            }
            for (const header of this.headers) {
                if (line.startsWith(`#${header}:`)) {
                    const value = line.replace(`#${header}:`, '').trim()
                    headers[header] = header === 'Fields' ? value.split(' ') : value
                }
            }
        }
        return headers
    }

    private static lineToObject(line: string, fields: string[], delimiter: string) {
        const columns = line.trim().split(delimiter)
        const result: object = {}
        for (let i = 0; i < fields.length; i++) {
            result[fields[i]] = columns[i]
        }
        return result
    }
}