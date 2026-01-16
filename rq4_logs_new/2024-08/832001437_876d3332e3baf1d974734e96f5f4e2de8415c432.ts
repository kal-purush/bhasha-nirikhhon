'use server';
import { OnBoardingType, stepTwoSchema, stepOneSchema, stepThreeSchema } from '../_component/schemas';
import { OnBoardingRoutes } from '../_component/path-types';
import { db } from '@rocket-house-productions/integration';
import { auth } from '@clerk/nextjs/server';
import { redirect } from 'next/navigation';

interface SubmitDealActionReturnType {
  redirect?: OnBoardingRoutes;
  errorMsg?: string;
  success?: boolean;
}

export const submitOnBoardingAction = async (
  onboarding: OnBoardingType,
  baseUrl: string,
): Promise<SubmitDealActionReturnType> => {
  console.log('submitOnBoardingAction', onboarding);

  const stepOneValidated = stepOneSchema.safeParse(onboarding);
  if (!stepOneValidated.success) {
    console.log('stepOneValidated', stepOneValidated.error);
    return {
      redirect: OnBoardingRoutes.PARENT_DETAILS,
      errorMsg: 'Please validate parent information.',
    };
  }

  const stepTwoValidated = stepTwoSchema.safeParse(onboarding);
  if (!stepTwoValidated.success) {
    console.log('stepTwoValidated', stepTwoValidated);
    return {
      redirect: OnBoardingRoutes.CHILD_DETAILS,
      errorMsg: 'Please validate child information.',
    };
  }

  const stepThreeValidated = stepThreeSchema.safeParse(onboarding);

  if (!stepThreeValidated.success) {
    console.log('stepThreeValidated', stepThreeValidated);
    return {
      redirect: OnBoardingRoutes.CHILD_AVATAR,
      errorMsg: 'Please validate avatar.',
    };
  }

  // Add child
  try {
    const { userId } = auth();

    if (!userId) {
      return redirect('/course/error?status=unauthorized');
    }

    const account = await db.account.findFirst({
      where: {
        userId: userId,
      },
    });

    if (!account) {
      return redirect('/course/error?status=unauthorized');
    }

    const child = await db.child.create({
      data: {
        name: onboarding.name,
        birthday: new Date(onboarding.birthday).toISOString(),
        profilePicture: onboarding.avatar,
        parentConsent: onboarding.parentConsent,
        notifications: onboarding.notify,
        account: {
          connect: {
            id: account.id,
          },
        },
      },
    });

    await db.purchase.update({
      where: {
        id: baseUrl.split('/').pop(),
      },
      data: {
        childId: child.id,
      },
    });
  } catch (error) {
    console.error('[ONBOARDING] [REVIEW] Error updating purchase with child id', error);

    return {
      redirect: OnBoardingRoutes.REVIEW,
      errorMsg: 'Error Update error',
    };
  }

  // Update purchase with child id

  const retVal = { success: true, redirect: OnBoardingRoutes.COMPLETED };
  console.log(retVal);
  return retVal;
};