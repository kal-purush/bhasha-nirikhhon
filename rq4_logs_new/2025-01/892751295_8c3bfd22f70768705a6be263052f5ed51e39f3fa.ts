import {createAsyncThunk} from '@reduxjs/toolkit'
import {fetchChecklists} from 'sr/utils/api/checklistApi'
import {fetchTaskList} from 'sr/utils/api/fetchTaskList'
export const fetchTaskData = createAsyncThunk('task/fetchTaskData', async (payload: any) => {
  let res = await fetchTaskList({})
  const response = await fetchChecklists({...payload, limit: res.pagination.total})
  const data: {task_name: string; id: string}[] = []
  const idNameMap: {[key: string]: string} = {}
  response.data.forEach((task) => {
    idNameMap[task.id] = task.name
    data.push({
      task_name: task.name,
      id: task.id,
    })
  })
  return {
    data,
    idNameMap,
  }
})