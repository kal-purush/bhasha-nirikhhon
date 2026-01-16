import mounty from 'mounty'

// This example demonstrates the proposed API.
mounty('/', {
  // Alias (i.e. redirect)
  '': 'home',

  // Constant mounting
  'home': <HomePage />,

  // Dynamic mounting
  'profile/{id}': (route) => <ProfilePage id={route.params.id} />,

  // Nested mounting
  'about': {
    // Alias within "about"
    '': 'team',
    'team': <AboutTeam />,
    'company': <AboutCompany />,
  },

  // Async mounting
  'feed': () => {
    return System.import('./components/Feed').then(Feed => <Feed />)
  },
})