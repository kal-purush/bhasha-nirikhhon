export interface SearchBoxProps {
  setSearched: (result: boolean) => void;
}

export interface SearchContainerProps {
  onLinkPressed: (result: boolean, title: string) => void;
}