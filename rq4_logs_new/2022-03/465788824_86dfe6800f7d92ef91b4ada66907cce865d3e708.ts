const
    outputFile = "VERSION",
    outputTS = "src/version.ts"
    

import gd from "git-describe"
import { writeFileSync } from "fs"

function getGitVersion(fallback = "not-git-repo-and-VERSION-not-set") {
    if (process.env.VERSION) {
        console.log("Reading version from environment VERSION")
        return process.env.VERSION
    }
    var version = fallback
    try {
        const git = gd.gitDescribeSync()
        if (!git.dirty && git.distance == 0) { // releases should have a plain vXX version
            version = git.tag
        } else {
            version = git.raw
        }
    } catch (error) {
        console.warn(error + "\n" +
            `not a git repository, using ${version} as version`)
    }
    return version
}

const version = getGitVersion()

const versionts = `
export const VERSION:string = "${version}"
`

try {

    writeFileSync(outputFile, version)
    console.log(`Set ${version} in >${outputFile}<`)

    writeFileSync(outputTS, versionts)
    console.log(`Set ${version} JS in >${outputTS}`)
} catch (err) {
    console.error(err)
    process.exit(1)
}