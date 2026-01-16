import { 
  LeaderboardPlayer, 
  MonthlyChallenge, 
  PlayerSpotlight,
  BadgeIconMap,
  BadgeColorMap,
  BadgeDescriptionMap
} from '@/types/leaderboard';
import { 
  Zap, 
  Trophy, 
  BrainCog, 
  Dumbbell, 
  Shield, 
  Crown, 
  Star, 
  Swords, 
  GraduationCap,
  LucideIcon
} from 'lucide-react';

// Badge icons mapping
export const badgeIcons: BadgeIconMap = {
  'combo': Trophy as unknown as React.ComponentType<any>,
  'reaction': Zap as unknown as React.ComponentType<any>,
  'adaptation': BrainCog as unknown as React.ComponentType<any>,
  'training': Dumbbell as unknown as React.ComponentType<any>,
  'clutch': Star as unknown as React.ComponentType<any>,
  'defense': Shield as unknown as React.ComponentType<any>,
  'grappling': Swords as unknown as React.ComponentType<any>,
  'lab': GraduationCap as unknown as React.ComponentType<any>,
  'legend': Crown as unknown as React.ComponentType<any>,
  'default': Trophy as unknown as React.ComponentType<any>
};

// Badge colors based on tier
export const badgeColors: BadgeColorMap = {
  'Diamond': 'bg-purple-600',
  'Platinum': 'bg-blue-600',
  'Gold': 'bg-yellow-600',
  'Silver': 'bg-gray-500',
  'Bronze': 'bg-amber-700'
};

// Badge descriptions
export const badgeDescriptions: BadgeDescriptionMap = {
  'combo': 'Awarded for mastering complex combo executions with high precision',
  'reaction': 'Recognizes exceptional reaction time in defensive and offensive situations',
  'adaptation': 'Demonstrates superior ability to adapt to opponent strategies mid-match',
  'training': 'Recognizes consistent training discipline and dedication',
  'clutch': 'Awarded for exceptional performance under pressure',
  'defense': 'Recognizes superior defensive skills and blocking techniques',
  'grappling': 'Demonstrates mastery of grappling and throw techniques',
  'lab': 'Awarded for innovative lab work and matchup knowledge',
  'legend': 'Recognizes legendary status in the Street Fighter community'
};

// Leaderboard players data
export const leaderboardPlayers: LeaderboardPlayer[] = [
  {
    id: 1,
    username: 'DaigoTheGreat',
    character: 'Ryu',
    country: 'Japan',
    countryFlag: '🇯🇵',
    rankTier: 'Master',
    rankPoints: 8750,
    reactionTime: 0.21,
    comboAccuracy: 97.5,
    adaptationScore: 95,
    avatarUrl: 'https://placehold.co/100?text=Ryu',
    badges: [
      { type: 'legend', name: 'Living Legend', tier: 'Diamond' },
      { type: 'reaction', name: 'Lightning Reflexes', tier: 'Diamond' },
      { type: 'adaptation', name: 'Strategy Master', tier: 'Diamond' },
      { type: 'defense', name: 'Perfect Blocker', tier: 'Diamond' }
    ],
    matchHistory: {
      wins: 342,
      losses: 29,
      draws: 3
    },
    trainingStreak: 145,
    improvementScore: 12
  },
  {
    id: 2,
    username: 'Tokido',
    character: 'Akuma',
    country: 'Japan',
    countryFlag: '🇯🇵',
    rankTier: 'Master',
    rankPoints: 8520,
    reactionTime: 0.22,
    comboAccuracy: 96.8,
    adaptationScore: 94,
    avatarUrl: 'https://placehold.co/100?text=Akuma',
    badges: [
      { type: 'adaptation', name: 'Strategy Master', tier: 'Diamond' },
      { type: 'combo', name: 'Combo Expert', tier: 'Diamond' },
      { type: 'clutch', name: 'Clutch Performer', tier: 'Diamond' }
    ],
    matchHistory: {
      wins: 315,
      losses: 41,
      draws: 2
    },
    trainingStreak: 89,
    improvementScore: 8
  },
  {
    id: 3,
    username: 'Infiltration',
    character: 'Juri',
    country: 'South Korea',
    countryFlag: '🇰🇷',
    rankTier: 'Master',
    rankPoints: 8350,
    reactionTime: 0.23,
    comboAccuracy: 95.3,
    adaptationScore: 93,
    avatarUrl: 'https://placehold.co/100?text=Juri',
    badges: [
      { type: 'adaptation', name: 'Strategy Master', tier: 'Diamond' },
      { type: 'lab', name: 'Lab Scientist', tier: 'Diamond' },
      { type: 'grappling', name: 'Throw Master', tier: 'Platinum' }
    ],
    matchHistory: {
      wins: 302,
      losses: 47,
      draws: 5
    },
    trainingStreak: 67,
    improvementScore: 7
  },
  {
    id: 4,
    username: 'Justin_Wong',
    character: 'Karin',
    country: 'United States',
    countryFlag: '🇺🇸',
    rankTier: 'Master',
    rankPoints: 8120,
    reactionTime: 0.24,
    comboAccuracy: 94.8,
    adaptationScore: 92,
    avatarUrl: 'https://placehold.co/100?text=Karin',
    badges: [
      { type: 'clutch', name: 'Clutch Performer', tier: 'Diamond' },
      { type: 'combo', name: 'Combo Expert', tier: 'Diamond' },
      { type: 'legend', name: 'Community Icon', tier: 'Platinum' }
    ],
    matchHistory: {
      wins: 295,
      losses: 52,
      draws: 4
    },
    trainingStreak: 54,
    improvementScore: 6
  },
  {
    id: 5,
    username: 'ProblemX',
    character: 'M. Bison',
    country: 'United Kingdom',
    countryFlag: '🇬🇧',
    rankTier: 'Diamond',
    rankPoints: 7980,
    reactionTime: 0.25,
    comboAccuracy: 93.5,
    adaptationScore: 90,
    avatarUrl: 'https://placehold.co/100?text=Bison',
    badges: [
      { type: 'combo', name: 'Combo Expert', tier: 'Diamond' },
      { type: 'defense', name: 'Perfect Blocker', tier: 'Platinum' }
    ],
    matchHistory: {
      wins: 267,
      losses: 63,
      draws: 7
    },
    trainingStreak: 42,
    improvementScore: 9
  },
  {
    id: 6,
    username: 'Momochi',
    character: 'Ken',
    country: 'Japan',
    countryFlag: '🇯🇵',
    rankTier: 'Diamond',
    rankPoints: 7760,
    reactionTime: 0.26,
    comboAccuracy: 93.1,
    adaptationScore: 88,
    avatarUrl: 'https://placehold.co/100?text=Ken',
    badges: [
      { type: 'reaction', name: 'Lightning Reflexes', tier: 'Platinum' },
      { type: 'training', name: 'Training Devotee', tier: 'Diamond' }
    ],
    matchHistory: {
      wins: 253,
      losses: 71,
      draws: 4
    },
    trainingStreak: 36,
    improvementScore: 5
  },
  {
    id: 7,
    username: 'NuckleDu',
    character: 'Guile',
    country: 'United States',
    countryFlag: '🇺🇸',
    rankTier: 'Diamond',
    rankPoints: 7590,
    reactionTime: 0.26,
    comboAccuracy: 92.2,
    adaptationScore: 87,
    avatarUrl: 'https://placehold.co/100?text=Guile',
    badges: [
      { type: 'defense', name: 'Perfect Blocker', tier: 'Diamond' },
      { type: 'reaction', name: 'Lightning Reflexes', tier: 'Platinum' }
    ],
    matchHistory: {
      wins: 248,
      losses: 75,
      draws: 5
    },
    trainingStreak: 31,
    improvementScore: 8
  },
  {
    id: 8,
    username: 'Oil_King',
    character: 'Rashid',
    country: 'Taiwan',
    countryFlag: '🇹🇼',
    rankTier: 'Diamond',
    rankPoints: 7420,
    reactionTime: 0.27,
    comboAccuracy: 91.8,
    adaptationScore: 86,
    avatarUrl: 'https://placehold.co/100?text=Rashid',
    badges: [
      { type: 'combo', name: 'Combo Expert', tier: 'Platinum' },
      { type: 'adaptation', name: 'Strategy Master', tier: 'Gold' }
    ],
    matchHistory: {
      wins: 235,
      losses: 82,
      draws: 3
    },
    trainingStreak: 28,
    improvementScore: 10
  },
  {
    id: 9,
    username: 'Punk',
    character: 'Cammy',
    country: 'United States',
    countryFlag: '🇺🇸',
    rankTier: 'Diamond',
    rankPoints: 7290,
    reactionTime: 0.25,
    comboAccuracy: 93.2,
    adaptationScore: 89,
    avatarUrl: 'https://placehold.co/100?text=Cammy',
    badges: [
      { type: 'reaction', name: 'Lightning Reflexes', tier: 'Diamond' },
      { type: 'clutch', name: 'Clutch Performer', tier: 'Platinum' }
    ],
    matchHistory: {
      wins: 241,
      losses: 68,
      draws: 2
    },
    trainingStreak: 25,
    improvementScore: 7
  },
  {
    id: 10,
    username: 'Fujimura',
    character: 'Ibuki',
    country: 'Japan',
    countryFlag: '🇯🇵',
    rankTier: 'Diamond',
    rankPoints: 7180,
    reactionTime: 0.24,
    comboAccuracy: 94.5,
    adaptationScore: 87,
    avatarUrl: 'https://placehold.co/100?text=Ibuki',
    badges: [
      { type: 'combo', name: 'Combo Expert', tier: 'Diamond' },
      { type: 'lab', name: 'Lab Scientist', tier: 'Platinum' }
    ],
    matchHistory: {
      wins: 232,
      losses: 71,
      draws: 4
    },
    trainingStreak: 38,
    improvementScore: 9
  },
  {
    id: 11,
    username: 'Phenom',
    character: 'Necalli',
    country: 'Norway',
    countryFlag: '🇳🇴',
    rankTier: 'Platinum',
    rankPoints: 6950,
    reactionTime: 0.27,
    comboAccuracy: 90.2,
    adaptationScore: 84,
    avatarUrl: 'https://placehold.co/100?text=Necalli',
    badges: [
      { type: 'adaptation', name: 'Strategy Master', tier: 'Platinum' }
    ],
    matchHistory: {
      wins: 215,
      losses: 82,
      draws: 5
    },
    trainingStreak: 19,
    improvementScore: 11
  },
  {
    id: 12,
    username: 'MenaRD',
    character: 'Birdie',
    country: 'Dominican Republic',
    countryFlag: '🇩🇴',
    rankTier: 'Platinum',
    rankPoints: 6820,
    reactionTime: 0.28,
    comboAccuracy: 88.5,
    adaptationScore: 85,
    avatarUrl: 'https://placehold.co/100?text=Birdie',
    badges: [
      { type: 'clutch', name: 'Clutch Performer', tier: 'Platinum' },
      { type: 'grappling', name: 'Throw Master', tier: 'Gold' }
    ],
    matchHistory: {
      wins: 198,
      losses: 86,
      draws: 3
    },
    trainingStreak: 21,
    improvementScore: 13
  },
  {
    id: 13,
    username: 'CoolKid93',
    character: 'Abigail',
    country: 'United States',
    countryFlag: '🇺🇸',
    rankTier: 'Platinum',
    rankPoints: 6730,
    reactionTime: 0.29,
    comboAccuracy: 87.8,
    adaptationScore: 83,
    avatarUrl: 'https://placehold.co/100?text=Abigail',
    badges: [
      { type: 'grappling', name: 'Throw Master', tier: 'Platinum' }
    ],
    matchHistory: {
      wins: 187,
      losses: 92,
      draws: 4
    },
    trainingStreak: 15,
    improvementScore: 8
  },
  {
    id: 14,
    username: 'SnakeEyez',
    character: 'Zangief',
    country: 'United States',
    countryFlag: '🇺🇸',
    rankTier: 'Platinum',
    rankPoints: 6610,
    reactionTime: 0.30,
    comboAccuracy: 86.5,
    adaptationScore: 82,
    avatarUrl: 'https://placehold.co/100?text=Zangief',
    badges: [
      { type: 'grappling', name: 'Throw Master', tier: 'Diamond' },
      { type: 'defense', name: 'Perfect Blocker', tier: 'Gold' }
    ],
    matchHistory: {
      wins: 182,
      losses: 95,
      draws: 2
    },
    trainingStreak: 17,
    improvementScore: 6
  },
  {
    id: 15,
    username: 'Luffy',
    character: 'R. Mika',
    country: 'France',
    countryFlag: '🇫🇷',
    rankTier: 'Platinum',
    rankPoints: 6520,
    reactionTime: 0.28,
    comboAccuracy: 88.2,
    adaptationScore: 84,
    avatarUrl: 'https://placehold.co/100?text=R.Mika',
    badges: [
      { type: 'grappling', name: 'Throw Master', tier: 'Platinum' },
      { type: 'clutch', name: 'Clutch Performer', tier: 'Gold' }
    ],
    matchHistory: {
      wins: 175,
      losses: 87,
      draws: 5
    },
    trainingStreak: 23,
    improvementScore: 9
  }
];

// Derived data
export const countries = Array.from(new Set(leaderboardPlayers.map(player => player.country)));
export const characters = Array.from(new Set(leaderboardPlayers.map(player => player.character)));
export const rankTiers = ['Rookie', 'Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond', 'Master'];

// Monthly Challenge
export const monthlyChallenge: MonthlyChallenge = {
  id: 1,
  title: "April Adaptation Challenge",
  description: "Improve your mid-match adaptation score by analyzing opponents' patterns and adjusting your strategy accordingly.",
  endDate: "2025-04-30",
  progress: 67,
  participants: 358,
  reward: {
    type: 'adaptation',
    name: 'Adaptation Master',
    tier: 'Platinum'
  }
};

// Player Spotlights
export const playerSpotlights: PlayerSpotlight[] = [
  {
    id: 1,
    playerId: 5,
    achievement: "Biggest Improvement",
    statType: 'combo',
    statChange: 18
  },
  {
    id: 2,
    playerId: 12,
    achievement: "Rising Star",
    statType: 'adaptation',
    statChange: 23
  },
  {
    id: 3,
    playerId: 8,
    achievement: "Tech Innovator",
    statType: 'reaction',
    statChange: 15
  }
];