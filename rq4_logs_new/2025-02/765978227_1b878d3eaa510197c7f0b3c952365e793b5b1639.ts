import { useRouter } from 'next/navigation';

import { toast } from '@/hooks/use-toast';

import { credentialsLogin } from '../../actions/login';

export const useCredentialLogin = () => {
  const router = useRouter();
  // ! Server Actionsの関数をラップし、useActionStateに合った関数を作って呼び出す
  const trigger = async (_prevState: object, formData: FormData) => {
    const res = await credentialsLogin(_prevState, formData);
    if (res.status === 'error') {
      toast({
        variant: 'destructive',
        description: 'ログインできませんでした',
      });
    } else {
      toast({ description: 'ログインしました' });
      router.push('/');
    }
    return {};
  };

  return { trigger };
};