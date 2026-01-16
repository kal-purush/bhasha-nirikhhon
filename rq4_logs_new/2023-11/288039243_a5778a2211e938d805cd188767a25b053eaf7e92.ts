import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const supabase_client = createClient(
  // Supabase API URL - env var exported by default when deployed.
  Deno.env.get('SUPABASE_URL') ?? '',
  // Supabase API SERVICE ROLE KEY - env var exported by default when deployed.
  Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
)
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

Deno.serve(async (req: any) => {
  try {
    // We need a user object and a room code
    const { user, room_code } = await req.json() // throws an error
    if (!user) throw new Error("Missing 'user' and/or 'room_code' in request body")

    // Get the match based on the passed room_code
    const { data: matchData, error: matchError } = await supabase_client
      .from('matches')
      .select('*')
      .eq('room_code', room_code)

    const { data: playersData, error: playersError } = await supabase_client
      .from('players')
      .upsert([
        {
          match_id: matchData[0].id,
          user_id: user.id,
        }
      ])

    return new Response(
      JSON.stringify(matchData[0]),
      { 
        headers: { "Content-Type": "application/json" },
        status: 200 
      },
    )
  } catch (error: any) {
    console.log(error)
    return new Response(JSON.stringify({ error: error.message }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      status: 400,
    })
  }
})