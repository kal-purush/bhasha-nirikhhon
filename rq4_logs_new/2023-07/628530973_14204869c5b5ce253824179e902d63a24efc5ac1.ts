import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useCallback } from 'react'
import { Availability, convertToSupabaseAvailability } from '../../models/service/Availability'
import { availabilityQueryKey, availabilityReference } from '../constants/QueryKeys'
import { supabase } from './SupabaseProvider'

const hookName = 'useUpdateAvailability'

export default function useUpdateAvailability() {
  const queryClient = useQueryClient()

  const mutation = useMutation({
    mutationFn: async ({ availabilityId, details }: { availabilityId: Availability['id']; details: Availability }) => {
      let supabaseAvailability = convertToSupabaseAvailability(details)

      // Check if it exists
      const { data } = await supabase.from(availabilityReference).select().eq('id', availabilityId)
      if (data) {
        const { error } = await supabase
          .from(availabilityReference)
          .update(supabaseAvailability)
          .eq('id', availabilityId)

        if (error) {
          console.log(`Error: ${hookName} updateAvailability `, error)
        }
      } else {
        const { error } = await supabase.from(availabilityReference).insert({ ...supabaseAvailability })

        if (error) {
          console.log(`Error: ${hookName} addAvailability `, error)
        }
      }

      return
    },
    onSuccess: () => {
      queryClient.invalidateQueries([availabilityQueryKey])
    },
  })

  const updateAvailability = useCallback(
    (availabilityId: Availability['id'], details: Availability) => {
      mutation.mutate({ availabilityId, details })
    },
    [mutation],
  )

  return {
    updateAvailability,
  }
}