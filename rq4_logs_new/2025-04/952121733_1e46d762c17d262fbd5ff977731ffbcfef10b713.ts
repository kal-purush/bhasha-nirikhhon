interface Game {
  id: string;              // Unique game ID (e.g., "ABC123")
  players: Player[];       // List of players
  targetScore: number;     // Winning score (e.g., 5)
  submissions: string[];   // Player responses
  currentPrompt: string;   // Black card question
  czar: string;            // Player ID of current judge
}

interface Player {
  id: string;
  name: string;
  score: number;
  isCzar: boolean;
}