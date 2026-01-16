export interface SupportArticle {
    id: number;
    title: string;
    summary: string;
    author: string;
    date: string;
    tags?: string[];
  }
  
  export const getSupportArticles = async (): Promise<SupportArticle[]> => {
    return Promise.resolve([
      {
        id: 101,
        title: "Getting Started with Storybook in React",
        summary:
          "Learn how to set up Storybook in a React project with Vite, build stories, and customize your environment.",
        author: "Emma Johansson",
        date: "2024-04-12",
        tags: ["Setup", "React", "Vite"]
      },
      {
        id: 102,
        title: "Storybook Addons: Boosting Developer Productivity",
        summary:
          "Explore essential Storybook addons like Actions, Controls, and Interactions that enhance component testing and documentation.",
        author: "Liam O'Connor",
        date: "2024-05-01",
        tags: ["Addons", "Productivity"]
      },
      {
        id: 103,
        title: "Mocking API Data in Storybook with MSW",
        summary:
          "This guide walks you through mocking REST and GraphQL APIs using Mock Service Worker (MSW) in Storybook.",
        author: "Sophia Lin",
        date: "2024-05-18",
        tags: ["MSW", "Mocking", "Testing"]
      },
      {
        id: 104,
        title: "Deploying Storybook to GitHub Pages",
        summary:
          "Step-by-step guide on how to deploy your Storybook build to GitHub Pages using GitHub Actions.",
        author: "Carlos Mendes",
        date: "2024-06-05",
        tags: ["CI/CD", "Deployment", "GitHub"]
      },
      {
        id: 105,
        title: "Using Storybook with TailwindCSS",
        summary:
          "Integrate TailwindCSS into your Storybook environment for consistent component styling and preview.",
        author: "Rina Patel",
        date: "2024-06-15",
        tags: ["Tailwind", "Styling"]
      }
    ]);
  };
  