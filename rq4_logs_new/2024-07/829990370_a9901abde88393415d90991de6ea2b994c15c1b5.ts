"use client"

import { useState, useEffect } from 'react'
import { Session } from '@supabase/supabase-js'
import { createClientComponentClient } from '@supabase/auth-helpers-nextjs'

export function useSupabaseAuth() {
  const [session, setSession] = useState<Session | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const supabase = createClientComponentClient()
    let mounted = true

    async function getInitialSession() {
      try {
        const {
          data: { session },
        } = await supabase.auth.getSession()

        if (mounted) {
          setSession(session)
          setLoading(false)
        }
      } catch (error) {
        console.error('Error fetching initial session:', error)
        if (mounted) {
          setLoading(false)
        }
      }
    }

    getInitialSession()

    const { data: authListener } = supabase.auth.onAuthStateChange(
      async (event, session) => {
        if (mounted) {
          setSession(session)
          setLoading(false)
        }
      }
    )

    return () => {
      mounted = false
      authListener.subscription.unsubscribe()
    }
  }, [])

  return { session, loading }
}