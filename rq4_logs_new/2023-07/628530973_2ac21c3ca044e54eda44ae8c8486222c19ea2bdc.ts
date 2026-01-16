import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Timestamp,
  addDoc,
  and,
  collection,
  doc,
  getDoc,
  getDocs,
  or,
  query,
  updateDoc,
  where,
} from 'firebase/firestore'
import _ from 'lodash'
import { DateTime } from 'luxon'
import { useCallback, useMemo } from 'react'
import Service, {
  FirebaseService,
  HourMinute,
  serviceFromSnapshot,
} from '../../models/service/Service'
import { servicesQueryKey, servicesReference } from '../constants/FirebaseKeys'
import { db } from './FirebaseProvider'
import User, { UserRole } from '../../models/user/User'

export interface ServiceYearMonths {
  year: number
  startMonth: number
  endMonth: number
}

export function getCurrentServiceYearMonths(): ServiceYearMonths {
  const today = new Date()
  const currentMonth = today.getMonth() + 1
  return {
    year: today.getFullYear(),
    startMonth: currentMonth % 2 ? currentMonth : currentMonth - 1,
    endMonth: currentMonth % 2 ? currentMonth + 1 : currentMonth,
  }
}

export default function useService() {
  const queryClient = useQueryClient()

  // update
  const mutation = useMutation({
    mutationFn: ({
      serviceId,
      details,
    }: {
      serviceId: Service['id']
      details: Partial<Service>
    }) => {
      return updateDoc(doc(db, servicesReference, serviceId), details)
    },
    onSuccess: () => {
      queryClient.invalidateQueries([servicesQueryKey])
    },
  })

  const updateService = useCallback(
    (serviceId: Service['id'], details: Partial<Service>) => {
      mutation.mutate({ serviceId, details })
    },
    [mutation],
  )

  const updateServiceUserAssignment = useCallback(
    async (serviceId: Service['id'], userId: User['id'], userRole: UserRole | undefined) => {
      try {
        // Fetch the existing service details
        const serviceDocRef = doc(db, servicesReference, serviceId)
        const serviceSnapshot = await getDoc(serviceDocRef)
        const existingService = serviceSnapshot.data() as Service

        // Remove the userId from assignments if role is undefined
        if (userRole === undefined) {
          const { [userId]: _, ...updatedAssignments } = existingService.assignments
          await updateDoc(serviceDocRef, { assignments: updatedAssignments })
        } else {
          // Update the assignments object with the new userId and userRole
          const updatedAssignments = {
            ...existingService.assignments,
            [userId]: userRole,
          }
          await updateDoc(serviceDocRef, { assignments: updatedAssignments })
        }
        queryClient.invalidateQueries([servicesQueryKey])
      } catch (error) {
        console.error('Error updating service:', error)
      }
    },
    [queryClient],
  )

  const updateServiceLead = useCallback(
    async (serviceId: Service['id'], userId: User['id'] | undefined) => {
      try {
        const serviceDocRef = doc(db, servicesReference, serviceId)

        if (userId === undefined) {
          await updateDoc(serviceDocRef, { lead: '' })
        } else {
          await updateDoc(serviceDocRef, { lead: userId })
        }
        queryClient.invalidateQueries([servicesQueryKey])
      } catch (error) {
        console.error('Error updating service:', error)
      }
    },
    [queryClient],
  )

  // add service
  const addMutation = useMutation({
    mutationFn: async ({ details }: { details: Omit<Service, 'id'> }) => {
      return await addDoc(collection(db, servicesReference), details)
    },
    onSuccess: () => {
      queryClient.invalidateQueries([servicesQueryKey])
    },
  })

  const addService = useCallback(
    (details: Service) => {
      addMutation.mutate({ details })
    },
    [addMutation],
  )
  return {
    updateService,
    updateServiceUserAssignment,
    updateServiceLead,
    addService,
  }
}