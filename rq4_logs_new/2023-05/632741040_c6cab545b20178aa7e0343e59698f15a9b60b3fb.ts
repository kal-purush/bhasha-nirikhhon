import { Request, Response } from 'express'

import users from '../../../data/users'
import User from '../../user/model'

export const importData = async (req: Request, res: Response) => {
  try {
    await User.deleteMany({})

    const createdUsers = await User.insertMany(users)
    const adminUser = createdUsers[0]._id

    res.status(200).json({
      status: 'success',
      message: 'Data imported successfully',
    })

    console.log('Data Imported...')
    process.exit(0)
  } catch (error) {
    console.log('Error: ', error)
    res.status(400).json({
      status: 'fai;',
      message: 'Import failed',
    })
    process.exit()
  }
}

export const destroyData = async () => {
  try {
    await User.deleteMany({})

    console.log('Data Destroyed...')
    process.exit(0)
  } catch (error) {
    console.log('Error: ', error)
    process.exit()
  }
}