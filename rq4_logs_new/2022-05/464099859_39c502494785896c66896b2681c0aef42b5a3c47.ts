import { Profiles } from 'types/profile'
import { createAsyncThunk } from '@reduxjs/toolkit'

export const fetchProfiles = createAsyncThunk<Profiles, number>(
  'profiles/fetchProfiles',
  async (userID: number) => {
    try {
      // TODO: Create custom route on json-server to get all the profile share userID = 1
      const data = await fetch(`http://localhost:4000/profile/${userID}`)
      return await data.json()
    } catch (e) {
      throw new Error(`Fail to fetch profiles: ${e}`)
    }
  }
)