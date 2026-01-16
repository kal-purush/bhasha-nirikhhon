export interface ICardProps {
  isForceMobile?: boolean;
  isSelected: boolean;
  title: string;
  subtitle?: string | null;
  onClick?: null | (() => void);
  renderingStatusFunction?: null | (() => JSX.Element | null);
}