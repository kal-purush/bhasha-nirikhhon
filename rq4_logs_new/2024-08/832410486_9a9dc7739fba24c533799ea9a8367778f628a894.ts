import Coolsms from 'coolsms-node-sdk';

const coolsmsClient = new Coolsms(
  process.env.NEXT_PUBLIC_COOLSMS_API_KEY!,
  process.env.NEXT_PUBLIC_COOLSMS_API_SECRET!
);

export async function sendSms(to: string, text: string) {
  try {
    const result = await coolsmsClient.sendOne({
      to: to,
      from: process.env.NEXT_PUBLIC_COOLSMS_SENDER_PHONE!,
      text: text,
      autoTypeDetect: true, // autoTypeDetect 속성을 추가
    });

    return result;
  } catch (error) {
    console.error('Failed to send SMS:', error);
    throw new Error('Failed to send SMS');
  }
}