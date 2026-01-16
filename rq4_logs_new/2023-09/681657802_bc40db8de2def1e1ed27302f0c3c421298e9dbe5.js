const Filter = ({ filter, onFilterChange }) => {
  
     return (
    <div>
      Find contacts by name
      <input
        type="text"
        value={filter}
        onChange={(e) => onFilterChange(e.target.value)}
      />
    </div>
  );
};

export default Filter;