import { createClient } from '@supabase/supabase-js';

// Access environment variables
const supabaseUrl = import.meta.env.VITE_SUPABASE_URL;
const supabaseKey = import.meta.env.VITE_SUPABASE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('Supabase URL or Key is undefined');
} else {
  console.log(supabaseUrl);  // Should print the correct Supabase URL
  console.log(supabaseKey);  // Should print the correct Supabase Key
}

// Initialize Supabase client
export const supabase = createClient(supabaseUrl, supabaseKey);
