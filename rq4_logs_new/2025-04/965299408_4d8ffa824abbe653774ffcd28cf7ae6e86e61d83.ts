// ブログ記事の型定義
export type BlogPost = {
  id: string;
  slug: string;
  title: string;
  date: string;
  excerpt: string;
  content: string;
  author: string;
  tags: string[];
};

// ダミーのブログ記事データ（後で実際のCMSやデータベースに置き換え可能）
const DUMMY_POSTS: BlogPost[] = [
  {
    id: "1",
    slug: "getting-started-with-nextjs",
    title: "Next.jsを始めるための完全ガイド",
    date: "2025-04-10",
    excerpt:
      "Next.jsは強力なReactフレームワークです。このガイドで基礎から応用まで学びましょう。",
    content: `
# Next.jsを始めるための完全ガイド

Next.jsは、Reactベースのフルスタックフレームワークで、サーバーサイドレンダリング、静的サイト生成、ルーティングなど多くの機能を提供します。

## インストール方法

\`\`\`bash
npx create-next-app@latest my-app
\`\`\`

## 基本的な使い方

Pagesを作成することで、自動的にルーティングが設定されます。
    `,
    author: "山田太郎",
    tags: ["Next.js", "React", "Web開発"],
  },
  {
    id: "2",
    slug: "static-site-generation-benefits",
    title: "SSGの利点：なぜ静的サイト生成が重要なのか",
    date: "2025-04-12",
    excerpt:
      "静的サイト生成のメリットと、それがブログやコンテンツサイトにとって最適な選択肢である理由を解説します。",
    content: `
# SSGの利点：なぜ静的サイト生成が重要なのか

静的サイト生成（SSG）は、ビルド時にページを生成するアプローチです。このアプローチにはいくつかの重要な利点があります。

## パフォーマンス

事前にレンダリングされたHTMLファイルを提供するため、ページの読み込み速度が高速です。

## セキュリティ

攻撃対象となるサーバーサイドのコードがないため、セキュリティリスクが低減されます。
    `,
    author: "佐藤花子",
    tags: ["SSG", "パフォーマンス", "Web開発"],
  },
];

// すべてのブログ記事を取得する関数
export async function getAllPosts(): Promise<BlogPost[]> {
  // 実際のアプリケーションでは、ここでCMSやデータベースからデータを取得
  // 現在はダミーデータを返す
  return DUMMY_POSTS;
}

// スラッグに基づいて単一の記事を取得する関数
export async function getPostBySlug(
  slug: string,
): Promise<BlogPost | undefined> {
  // 実際のアプリケーションでは、ここで特定のスラッグに一致する記事をCMSやデータベースから取得
  return DUMMY_POSTS.find((post) => post.slug === slug);
}

// すべての記事のスラッグのリストを取得する関数（静的パスの生成に使用）
export async function getAllPostSlugs(): Promise<string[]> {
  const posts = await getAllPosts();
  return posts.map((post) => post.slug);
}