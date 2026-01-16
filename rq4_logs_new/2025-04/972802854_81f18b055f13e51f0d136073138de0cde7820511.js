const { createClient } = require("@supabase/supabase-js");


const supabseUrl = process.env.SUPABASE_URL || "https://rjjdutprvrlrtmjtesfg.supabase.co"
const supabaseKey = process.env.SUPABSE_ANON_KEY || "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJqamR1dHBydnJscnRtanRlc2ZnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDI1Mjg4MTMsImV4cCI6MjA1ODEwNDgxM30.yIu03W0BVWisiVF0FhqEC1Kl6U3gKNusUin6DBMu3TU"

const supabse = createClient(supabseUrl, supabaseKey)

if (!supabseUrl || !supabaseKey) {
    throw new Error("Missing Supabase Environmental Variables");
}


module.exports = supabse