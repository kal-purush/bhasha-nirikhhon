import ACHIEVEMENT_ACTION_TYPE_ENUM, {
  AchievementActionType,
} from './achievementActionTypeEnum';

export type AchievementContents = {
  title: string;
  body: string;
  actions: AchievementActionType[];
};

const ACHIEVEMENT_TEXT: {
  present: {
    newCharacter: AchievementContents[];
    newSkin: AchievementContents;
  };
  levelUp: {};
  completeDailyGoal: {};
} = {
  present: {
    newCharacter: [
      {
        title: 'Get the Character！',
        body: 'おめでとうございます！目標を5日間連続で達成したので、ガチャを引いてキャラクターをゲットできます！',
        actions: [ACHIEVEMENT_ACTION_TYPE_ENUM.GACHA],
      },
      {
        title: 'New Character！',
        body: '“ハリセンボン”のキャラクターをゲットしました！リポジトリを設定して自分だけのキャラクターを育てましょう。',
        actions: [
          ACHIEVEMENT_ACTION_TYPE_ENUM.COLLECTION,
          ACHIEVEMENT_ACTION_TYPE_ENUM.REPOSITORY,
        ],
      },
    ],
    newSkin: {
      title: 'New Skin！',
      body: 'おめでとうございます！今週は作業量が多かったので、新しいスキンをプレゼントします！',
      actions: [ACHIEVEMENT_ACTION_TYPE_ENUM.COLLECTION],
    },
  },
  levelUp: {},
  completeDailyGoal: {},
};

export default ACHIEVEMENT_TEXT;