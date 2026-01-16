// 環境変数が存在するか確認します
function getEnvVar(key: keyof NodeJS.ProcessEnv): string {
    const value = process.env[key];
    // 環境変数が存在しない場合はエラーとします
    if (typeof value === 'undefined') {
      throw new Error(`Environment variable ${key} is not defined.`);
    }
    return value;
}


// 外部から読み込む環境変数をチェックします
export const NEXT_PUBLIC_GITHUB_URL = getEnvVar("NEXT_PUBLIC_GITHUB_URL");
export const NEXT_PUBLIC_TWITTER_URL = getEnvVar("NEXT_PUBLIC_TWITTER_URL");
export const NEXT_PUBLIC_BLOG_NAME = getEnvVar("NEXT_PUBLIC_BLOG_NAME");