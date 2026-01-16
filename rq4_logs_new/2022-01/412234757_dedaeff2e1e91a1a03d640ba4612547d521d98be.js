const TimeBar = ({ time }) => {
  return (
    <div className='timebar'>
      <div className='timebar__time' style={{ width: `${time}%` }}></div>
    </div>
  );
};

export default TimeBar;