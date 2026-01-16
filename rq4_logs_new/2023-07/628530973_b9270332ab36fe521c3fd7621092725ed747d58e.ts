import { createClient } from '@supabase/supabase-js'
import { useEffect, useState } from 'react'

// Setup
const supabaseUrl = 'https://lxnkjsazarmoeuyfhtqz.supabase.co'
// const supabaseKey = process.env.SUPABASE_KEY
const supabaseKey =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx4bmtqc2F6YXJtb2V1eWZodHF6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2ODk2NDY1MzksImV4cCI6MjAwNTIyMjUzOX0.IxflI4fV5eD_QO85RQpmpUoTKYLVx7GzxrYYwgzQGeI'
export const supabase = createClient(supabaseUrl, supabaseKey)

export const signInWithGoogleSupa = async () => {
  const { data, error } = await supabase.auth.signInWithOAuth({
    provider: 'google',
    options: {
      queryParams: {
        access_type: 'offline',
        prompt: 'consent',
      },
    },
  })
  if (error !== null) {
    console.log('error')
    console.log(error)
  }
  console.log('success')
  console.log(data)
  // should save token
}