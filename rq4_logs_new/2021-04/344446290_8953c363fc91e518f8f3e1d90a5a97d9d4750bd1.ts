import { exec } from 'child_process'
import path from 'path'
import { createReadStream, createWriteStream, unlinkSync } from 'fs'
import { GetADUsersOptions, GetADUsersResult } from './types'

const binaryScriptPath = path.join(__dirname, '../../src/data/get-aduser.ps1')
const scriptDirectory = path.dirname(process.execPath)
const realScriptPath = path.join(scriptDirectory, './get-aduser.ps1')

// console.log(`
// binaryScriptPath: ${binaryScriptPath}
// scriptDirectory: ${scriptDirectory}
// realScriptPath: ${realScriptPath}
// `)

let timer: NodeJS.Timeout | undefined
function deployScript (timeout: number = 3000): void {
  createReadStream(binaryScriptPath).pipe(createWriteStream(realScriptPath))

  if (typeof timer !== 'undefined') clearTimeout(timeout)
  timer = setTimeout(() => {
    unlinkSync(realScriptPath)
  }, timeout)
}

export async function getADUsers (options: GetADUsersOptions): Promise<GetADUsersResult> {
  deployScript()

  const filter = createFilter(options.users)
  return await new Promise((resolve, reject) => {
    const command = `${realScriptPath} '${filter}'`
    exec(command, { shell: 'powershell.exe' }, (error, stdout) => {
      if (error !== null) {
        reject(error)
        return
      }
      try {
        const users = JSON.parse(stdout)
        const formattedUsers = users.map((user: any) => ({
          givenname: user.Givenname,
          surname: user.Surname,
          username: user.SamAccountName
        }))
        resolve({ users: formattedUsers })
      } catch (parseError) {
        reject(parseError)
      }
    })
  })
}

export async function getADUsersBulk (options: GetADUsersOptions): Promise<GetADUsersResult> {
  let bulkSize = Number(process.argv[2])
  if (isNaN(bulkSize)) bulkSize = 100

  const bulks: Array<GetADUsersOptions['users']> = []
  for (let index = 0; index < options.users.length; index += bulkSize) {
    const bulk = options.users.slice(index, index + bulkSize)
    bulks.push(bulk)
  }

  const users: GetADUsersResult['users'] = []
  for (const userBulk of bulks) {
    const bulkResult = await getADUsers({ users: userBulk })
    users.push(...bulkResult.users)
  }

  return { users }
}

function createFilter (users: GetADUsersOptions['users']): string {
  const userFilters = users.map(user => `((Givenname -eq "${user.givenname}") -and (Surname -eq "${user.surname}"))`)

  const filter = `(${userFilters.join(' -or ')})`

  return filter
}