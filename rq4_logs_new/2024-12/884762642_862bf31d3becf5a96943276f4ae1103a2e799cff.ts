import styled from 'styled-components';
import cameraIcon from '@assets/svgs/camera.svg';

export const Section = styled.div`
  margin-bottom: 3.2rem;
`;

export const ThumbnailLabel = styled.div`
  display: inline-block;
  ${({ theme }) => theme.fontStyles.Body4};
  margin-bottom: 1.2rem;
  color: ${({ theme }) => theme.colors.TZ_Monochrome[1000]};
`;

export const ThumbnailHint = styled.div`
  ${({ theme }) => theme.fontStyles.Caption7};
  color: ${({ theme }) => theme.colors.TZ_Monochrome[300]};
  margin-left: 0.8rem;
  margin-top: 0.2rem;
`;

export const MediaButton = styled.button`
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.6rem;
  padding: 0.8rem 1.2rem;
  border: 0.6px solid ${({ theme }) => theme.colors.TZ_Monochrome[500]};
  border-radius: 8px;
  background: none;
  ${({ theme }) => theme.fontStyles.Caption8};
  color: ${({ theme }) => theme.colors.TZ_Monochrome[500]};
  cursor: pointer;
  width: fit-content;
  height: fit-content;

  label {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    ${({ theme }) => theme.fontStyles.Caption8};
    cursor: pointer;
  }

  &:active {
    opacity: 0.8;
  }
`;

export const CameraIcon = styled.img.attrs({
  src: cameraIcon,
  alt: 'Camera',
})`
  width: 1.2rem;
  height: 1.1rem;
`;

export const Preview = styled.div`
  background-size: cover;
  background-position: center;
  width: 14.8rem;
  height: 14.8rem;
  border-radius: 8px;
  background-color: ${({ theme }) => theme.colors.TZ_Monochrome[100]};
`;

export const ViewSection = styled.div`
  display: flex;
  align-items: flex-end;
  gap: 1.1rem;
`;

export const TitleSection = styled.div`
  display: flex;
`;