export function TaskItemHandlers(
  id: number,
  setIsDone: React.Dispatch<React.SetStateAction<boolean>>
) {
  const handleClickPopUpDetail = () => {
    console.log('디테일');
    // 팝업 로직
  };

  const handleClickItemEdit = () => {
    console.log('수정하기');
  };

  const handleClickItemDelete = () => {
    console.log('삭제하기');
  };

  const handleClickItemStatusChange = () => {
    console.log('던');
    setIsDone((prev) => !prev);
  };

  const handleClickToggleDailyMode = () => {
    console.log('데일리모드로 전환');
  };

  return {
    handleClickPopUpDetail,
    handleClickItemEdit,
    handleClickItemDelete,
    handleClickItemStatusChange,
    handleClickToggleDailyMode,
  };
}