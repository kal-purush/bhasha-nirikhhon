export const stepTextData = [
  '서비스 이용 약관',
  '닉네임을 정해주세요',
  '고민을 알려주세요',
  '직업을 알려주세요',
  '성별을 알려주세요',
  '생일을 알려주세요',
  'mbti는 무엇인가요'
]

export const purposeData: string[] = ['학업/진로', '대인 관계', '성격/가치관', '행동/습관', '건강', '기타']
export const jobData: string[] = ['고등학생', '대학(원)생', '취업준비생', '직장인', '주부']
export const genderData: string[] = ['남성', '여성']
export const birthDateData: Array<{
  defaultText: string
  data: string[] | number[]
}> = [
  {
    defaultText: '년\u00a0\u00a0\u00a0',
    data: new Array(70).fill(0).map((_, i) => 2023 - i)
  },
  {
    defaultText: '월\u00a0',
    data: new Array(12).fill(0).map((_, i) => i + 1)
  },
  {
    defaultText: '일\u00a0',
    data: new Array(31).fill(0).map((_, i) => i + 1)
  }
]