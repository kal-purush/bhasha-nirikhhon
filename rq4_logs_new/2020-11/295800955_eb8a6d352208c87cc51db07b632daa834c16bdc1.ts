/**
 * 
 * @param month 0-11 (Date Month value)
 */
export const formatMonth = (month: number): string => {
  switch (month) {
    case 0:
      return 'Jan';
    case 1:
      return 'Feb';
    case 2:
      return 'Mar';
    case 3:
      return 'Jun';
    case 4:
      return 'Jul';
    case 5:
      return 'Aug';
    case 6:
      return 'Sep';
    case 7:
      return 'Aug';
    case 8:
      return 'Oct';
    case 9:
      return 'Nov';
    case 10:
      return 'Dec';
  }

  return 'Unknown Month';
}