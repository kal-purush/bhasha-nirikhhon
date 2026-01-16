import { NextResponse } from 'next/server';

const GITHUB_API_URL = 'https://api.github.com';
const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql';
const USERNAME = 'Ronit-Raj9';

export async function GET() {
  try {
    // Use GitHub token for higher rate limits - required for GraphQL API
    const headers: HeadersInit = {
      'Accept': 'application/vnd.github+json',
    };
    
    if (process.env.GITHUB_TOKEN) {
      headers['Authorization'] = `Bearer ${process.env.GITHUB_TOKEN}`;
    } else {
      // GraphQL API requires authentication
      return NextResponse.json(
        { error: 'GitHub token is required for fetching contribution data' },
        { status: 401 }
      );
    }

    // Fetch basic user data
    const userResponse = await fetch(`${GITHUB_API_URL}/users/${USERNAME}`, { 
      headers,
      next: { revalidate: 3600 } // Revalidate cache every hour
    });
    
    if (!userResponse.ok) {
      throw new Error(`GitHub API responded with status: ${userResponse.status}`);
    }
    const userData = await userResponse.json();

    // Fetch repositories data 
    const reposResponse = await fetch(`${GITHUB_API_URL}/users/${USERNAME}/repos?per_page=100&sort=updated`, { 
      headers,
      next: { revalidate: 3600 }
    });
    
    if (!reposResponse.ok) {
      throw new Error(`GitHub API responded with status: ${reposResponse.status}`);
    }
    const reposData = await reposResponse.json();

    // Calculate total stars
    const totalStars = reposData.reduce((total: number, repo: any) => {
      return total + repo.stargazers_count;
    }, 0);

    // Get top repositories by stars
    const topRepos = [...reposData]
      .sort((a, b) => b.stargazers_count - a.stargazers_count)
      .slice(0, 5)
      .map(repo => ({
        name: repo.name,
        stars: repo.stargazers_count,
        forks: repo.forks_count,
        url: repo.html_url,
        description: repo.description?.slice(0, 100) + (repo.description?.length > 100 ? '...' : '') || '',
        language: repo.language
      }));

    // Calculate language distribution
    const languages: Record<string, number> = {};
    let totalSize = 0;
    
    reposData.forEach((repo: any) => {
      if (repo.language && !repo.fork) {
        totalSize += repo.size;
        languages[repo.language] = (languages[repo.language] || 0) + repo.size;
      }
    });

    const languagePercentages = Object.entries(languages)
      .map(([name, size]) => ({
        name,
        percentage: Math.round((size / totalSize) * 100)
      }))
      .sort((a, b) => b.percentage - a.percentage)
      .slice(0, 5);

    // If less than 5 languages, add "Others" category
    if (languagePercentages.length < 5) {
      const topLanguagesPercentage = languagePercentages.reduce((total, lang) => total + lang.percentage, 0);
      if (topLanguagesPercentage < 100) {
        languagePercentages.push({
          name: 'Others',
          percentage: 100 - topLanguagesPercentage
        });
      }
    }

    // Fetch contribution data using GitHub GraphQL API
    // This will get the contribution calendar, commit counts, and contribution stats
    const graphqlQuery = {
      query: `
        query {
          user(login: "${USERNAME}") {
            name
            contributionsCollection {
              contributionCalendar {
                totalContributions
                weeks {
                  firstDay
                  contributionDays {
                    contributionCount
                    date
                    weekday
                  }
                }
              }
              totalCommitContributions
              totalIssueContributions
              totalPullRequestContributions
              totalPullRequestReviewContributions
            }
            repositories(first: 100, orderBy: {field: UPDATED_AT, direction: DESC}) {
              totalCount
              nodes {
                name
                defaultBranchRef {
                  target {
                    ... on Commit {
                      history {
                        totalCount
                      }
                    }
                  }
                }
              }
            }
          }
        }
      `
    };

    const graphqlResponse = await fetch(GITHUB_GRAPHQL_URL, {
      method: 'POST',
      headers: {
        ...headers,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(graphqlQuery),
      next: { revalidate: 3600 }
    });

    if (!graphqlResponse.ok) {
      throw new Error(`GitHub GraphQL API responded with status: ${graphqlResponse.status}`);
    }

    const graphqlData = await graphqlResponse.json();
    
    if (graphqlData.errors) {
      console.error('GraphQL Errors:', graphqlData.errors);
      throw new Error('Error in GraphQL response');
    }

    const contributionCalendar = graphqlData.data.user.contributionsCollection.contributionCalendar;
    const repositoryCommits = graphqlData.data.user.repositories.nodes
      .filter((repo: any) => repo.defaultBranchRef) // Filter out repos without a default branch
      .map((repo: any) => ({
        name: repo.name,
        commitCount: repo.defaultBranchRef?.target?.history?.totalCount || 0
      }))
      .sort((a: any, b: any) => b.commitCount - a.commitCount)
      .slice(0, 10); // Get top 10 repos by commit count

    // Format contribution calendar data
    const contributionDays = contributionCalendar.weeks.flatMap((week: any) => 
      week.contributionDays.map((day: any) => ({
        count: day.contributionCount,
        date: day.date,
        weekday: day.weekday
      }))
    );

    // Extract the weekly data directly for the calendar display
    const contributionWeeks = contributionCalendar.weeks.map((week: any) => ({
      firstDay: week.firstDay,
      days: week.contributionDays.map((day: any) => ({
        count: day.contributionCount,
        date: day.date,
        weekday: day.weekday
      }))
    }));

    // Group contributions by month for visualization
    const contributionsByMonth: Record<string, number> = {};
    contributionDays.forEach((day: any) => {
      const month = day.date.substring(0, 7); // YYYY-MM format
      contributionsByMonth[month] = (contributionsByMonth[month] || 0) + day.count;
    });

    // Real contribution stats
    const contributionStats = {
      total: contributionCalendar.totalContributions,
      code: graphqlData.data.user.contributionsCollection.totalCommitContributions,
      issues: graphqlData.data.user.contributionsCollection.totalIssueContributions,
      prs: graphqlData.data.user.contributionsCollection.totalPullRequestContributions + 
           graphqlData.data.user.contributionsCollection.totalPullRequestReviewContributions
    };

    // Return combined data
    return NextResponse.json({
      profile: {
        name: userData.name || USERNAME,
        avatarUrl: userData.avatar_url,
        followers: userData.followers,
        following: userData.following,
        publicRepos: userData.public_repos,
        totalStars,
        url: userData.html_url,
        bio: userData.bio
      },
      repos: reposData.length,
      topRepositories: topRepos,
      languages: languagePercentages,
      contributions: contributionStats,
      contributionCalendar: {
        totalContributions: contributionCalendar.totalContributions,
        days: contributionDays,
        weeks: contributionWeeks
      },
      contributionsByMonth,
      repositoryCommits,
      lastUpdated: new Date().toISOString(),
    });
  } catch (error) {
    console.error('Error fetching GitHub data:', error);
    return NextResponse.json(
      { error: 'Failed to fetch GitHub data' },
      { status: 500 }
    );
  }
} 