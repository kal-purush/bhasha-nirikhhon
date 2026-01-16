import fetch from "node-fetch";
import { BugsResponse } from "./bugs";
import { safeRun } from "./auth";

interface Summary {
    bugs: Number[],
    count: Number
}

interface ReleaseSummary extends Summary {
    [key: string]: any
}

function getStub(): Summary {
    return {
        bugs: [],
        count: 0
    }
}

export async function getSummary(product: string, targetMilestone: string): Promise<Summary> {
    return safeRun(async (token) => {
        const bugs = await (await fetch(`https://bugzilla.string.org.in/rest.cgi/bug?peoduct=${ product }&target_milestone=${ targetMilestone }&token=${ token }`)).json() as BugsResponse

        const response = {
            ...getStub()
        } as ReleaseSummary
    
        response.count = bugs.bugs.length
    
        for (let bug of bugs.bugs) {
            if (!response[bug.status]) {
                response[bug.status] = getStub()
            }
    
            if (!response[bug.status][bug.resolution]) {
                response[bug.status][bug.resolution] = getStub()
            }
    
            const statusObject = response[bug.status]
            statusObject.count = statusObject.bugs.push(bug.id)
    
            const resolutionObject = statusObject[bug.resolution]
            resolutionObject.count = resolutionObject.bugs.push(bug.id)
    
            response.bugs.push(bug.id)
        }
    
        return response
    }, 'Cannot get bugs')
}