import { createClient } from "@supabase/supabase-js"

export default function useSupabase() {
	const runtimeConfig = useRuntimeConfig()

	const supabaseUrl = runtimeConfig.public.supabaseUrl
	const supabaseKey = runtimeConfig.public.supabaseKey

	return createClient(supabaseUrl, supabaseKey)
}