import { useCallback } from 'react';
import { Option } from '@/components/common/multi-select';

export const useSkillValidation = () => {
  const validateSkills = useCallback(
    (teachSkills: Option[], learnSkills: Option[]): { isValid: boolean; message?: string } => {
      const teachSkillValues = teachSkills.map((skill) => skill.value);
      const learnSkillValues = learnSkills.map((skill) => skill.value);

      const duplicateSkills = teachSkillValues.filter((value) => learnSkillValues.includes(value));

      if (duplicateSkills.length > 0) {
        const duplicateSkillNames = duplicateSkills
          .map((value) => {
            const skill = [...teachSkills, ...learnSkills].find((s) => s.value === value);
            return skill?.label;
          })
          .filter(Boolean);

        return {
          isValid: false,
          message: `Skills ${duplicateSkillNames.join(', ')} cannot be both taught and learned`,
        };
      }

      return { isValid: true };
    },
    [],
  );

  const getFilteredOptions = useCallback((allOptions: Option[], oppositeSelectedOptions: Option[]): Option[] => {
    const oppositeSelectedValues = oppositeSelectedOptions.map((option) => option.value);
    const filtered = allOptions.filter((option) => !oppositeSelectedValues.includes(option.value));
    return filtered;
  }, []);

  return { validateSkills, getFilteredOptions };
};