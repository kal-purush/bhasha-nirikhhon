import * as Haptics from "expo-haptics";

export const triggerHapticFeedback = (
  style: keyof typeof Haptics.ImpactFeedbackStyle = "Light"
) => {
  Haptics.impactAsync(Haptics.ImpactFeedbackStyle[style]);
};