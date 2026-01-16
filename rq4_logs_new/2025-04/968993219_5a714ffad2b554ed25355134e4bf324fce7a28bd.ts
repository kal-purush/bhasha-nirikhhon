import useAuth from './useAuth';
import { Skill } from '@/types/skill.type';
import { Option } from '@/components/common/multi-select';
import { useEffect, useRef, useState } from 'react';
import { ProfileFormData, profileSchema } from '@/pages/EditProfile/profile.schema';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { useQuery } from '@tanstack/react-query';
import { GET_SKILLS_QUERY_KEY, getSkills } from '@/services/skill.service';
import { GET_SINGLE_USER, getUserByUID } from '@/services/user.service';
import { User } from '@/types/user.type';

const formatedOptions = (options: Skill[]) => {
  return options.map((skill) => ({
    label: skill.name,
    value: skill.id.toString(),
  }));
};

const subtractedOptions = (main: Option[], sub: Option[]) => {
  let p1 = 0;
  let p2 = 0;
  let newArr = main;

  while (p2 < sub.length) {
    if (main[p1].value !== sub[p2].value) {
      p1++;
    }
    newArr = newArr.filter((option: Option) => !(option.value === sub[p2].value));
    p2++, (p1 = 0);
  }

  return newArr;
};

const useEditProfile = () => {
  const { user } = useAuth();

  const { data: cur } = useQuery({
    queryKey: [GET_SINGLE_USER, user?.uid],
    queryFn: () => getUserByUID(user?.uid),
    enabled: !!user?.uid,
  });

  const { data: skills } = useQuery({
    queryKey: [GET_SKILLS_QUERY_KEY],
    queryFn: () => getSkills(),
  });

  const [currentUser, setCurrentUser] = useState<User>();
  const [skillOptions, setSkillOptions] = useState<Option[]>([]);
  const [avatar, setAvatar] = useState<string | null | undefined>();
  const [learn, setLearn] = useState<Option[]>([]);
  const [teach, setTeach] = useState<Option[]>([]);
  const [learnAvailable, setLearnAvailable] = useState<Option[]>(subtractedOptions(skillOptions, teach));
  const [teachAvailable, setTeachAvailable] = useState<Option[]>(subtractedOptions(skillOptions, learn));

  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleSelectPicture = () => {
    fileInputRef.current?.click();
  };

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      const imageUrl = URL.createObjectURL(file);
      setAvatar(imageUrl);
    }
  };

  const handleDeletePicture = () => {
    setAvatar('/placeholder.svg?height=200&width=200');
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const form = useForm<ProfileFormData>({
    resolver: zodResolver(profileSchema),
    defaultValues: {
      name: '',
      bio: '',
      learn: learn.map((option: Option) => option.value),
      teach: teach.map((option: Option) => option.value),
    },
  });

  const handleLearnSkillsChange = (options: Option[]) => {
    form.setValue(
      'learn',
      options.map((o) => o.value),
    );
    setLearn(options);
  };

  const handleTeachSkillsChange = (options: Option[]) => {
    form.setValue(
      'teach',
      options.map((o) => o.value),
    );
    setTeach(options);
  };

  function onSubmit(values: ProfileFormData) {
    console.log(values);
  }

  useEffect(() => {
    if (cur) {
      setCurrentUser(cur);
    }
  }, [cur]);

  useEffect(() => {
    if (skills) {
      setSkillOptions(formatedOptions(skills));
    }
  }, [skills]);

  useEffect(() => {
    if (currentUser && skillOptions.length !== 0) {
      const learningSkills =
        currentUser.learn.map((id: string) => skillOptions.filter((skill: Option) => skill.value === id)).flat();
      const teachingSkills =
        currentUser.teach.map((id: string) => skillOptions.filter((skill: Option) => skill.value === id)).flat();
      setLearn(learningSkills);
      setTeach(teachingSkills);
      setAvatar(currentUser.photoURL);
      form.reset({
        name: currentUser.fullName,
        bio: currentUser.bio,
        learn: learningSkills.map((option: Option) => option.value),
        teach: teachingSkills.map((option: Option) => option.value),
      });
    }
  }, [currentUser, skillOptions]);

  useEffect(() => {
    setLearnAvailable(subtractedOptions(skillOptions, teach));
    setTeachAvailable(subtractedOptions(skillOptions, learn));
  }, [learn, teach]);

  return {
    formatedOptions,
    subtractedOptions,
    form,
    avatar,
    setAvatar,
    learn,
    setLearn,
    teach,
    setTeach,
    learnAvailable,
    setLearnAvailable,
    teachAvailable,
    setTeachAvailable,
    handleDeletePicture,
    handleFileChange,
    handleLearnSkillsChange,
    handleSelectPicture,
    handleTeachSkillsChange,
    onSubmit,
    fileInputRef,
    skillOptions,
    currentUser,
  };
};

export default useEditProfile;