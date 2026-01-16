const AchievementText = {
  present: {
    newCharacter: {
      titles: ['Get the Character！', 'New Character！'],
      bodies: [
        'おめでとうございます！目標を5日間連続で達成したので、ガチャを引いてキャラクターをゲットできます！',
        (fishName: string) =>
          `“${fishName}”のキャラクターをゲットしました！リポジトリを設定して自分だけのキャラクターを育てましょう。`,
      ],
      actions: ['ガチャを引く', ['コレクション', 'リポジトリを設定する']],
    },
    newSkin: {
      title: 'New Skin！',
      body: 'おめでとうございます！今週は作業量が多かったので、新しいスキンをプレゼントします！',
      action: 'コレクション',
    },
  },
  levelUp: {},
  completeDailyGoal: {},
} as const;

export default AchievementText;