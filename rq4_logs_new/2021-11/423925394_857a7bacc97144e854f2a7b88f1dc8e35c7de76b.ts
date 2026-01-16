import { Title } from 'components/atoms/Title/Title';
import styled from 'styled-components';
import { Weather } from 'types/Weather.types';

const getBgcImage = (theme: any, weatherDesc: string) => {
  switch (weatherDesc) {
    case 'cloudy':
      return theme.backgroundImage.cloudy;
    case 'rainy':
      return theme.backgroundImage.cloudy;
    default:
      return theme.backgroundImage.sunny;
  }
};

const getColor = (theme: any, children: any) => {
  const temp = +children[0];

  if (temp > 15) return theme.colors.success;
  if (temp >= 10) return theme.colors.warning;
  if (temp < 10) return theme.colors.error;
  return theme.colors.darkPurple;
};

export const Wrapper = styled.li<Pick<Weather, 'weatherDesc'>>`
  width: 18%;
  display: flex;
  flex-direction: column;
  align-items: center;
  background-image: url(${({ theme, weatherDesc }) => getBgcImage(theme, weatherDesc)});
  background-size: cover;
  background-repeat: no-repeat;
  border-radius: 25px;
`;

export const InfoWrapper = styled.div`
  padding: 10px 20px;
`;

export const ValueParagraph = styled.p`
  font-size: ${({ theme }) => theme.fontSize.l};
  font-weight: bold;
  color: ${({ theme, children }) => getColor(theme, children)};
  margin-bottom: 0;
`;
export const SingleDayTitle = styled(Title)`
  color: ${({ theme }) => theme.colors.darkPurple};
`;