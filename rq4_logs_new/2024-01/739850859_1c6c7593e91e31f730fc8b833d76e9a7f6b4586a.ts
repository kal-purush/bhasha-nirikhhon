'use server'

import { Data } from '@/components/interface'

export async function getCategory(id: number) {
  if (!id) {
    return []
  }

  const res = await fetch('https://private-f2fbfb-ridecar2.apiary-mock.com/vehicles', {
    next: { revalidate: 900 }
  })

  if (!res.ok) {
    return []
  }

  const data: Data = await res.json()
  for (let i = 0; i < data.type.length; i++) {
    if (data.type[i].category_id === id) {
      return data.type[i].car_type.map((car) => {
        return {
          ...car,
          id: data.type[i].id
        }
      })
    }
  }

  return []
}
