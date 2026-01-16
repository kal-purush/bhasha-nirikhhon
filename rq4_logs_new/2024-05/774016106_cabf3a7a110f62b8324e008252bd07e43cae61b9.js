import { cloneElement, Children } from "react";
import { Autocomplete, createFilterOptions, TextField, Box, InputAdornment, SvgIcon } from "@mui/material";
import PropTypes from 'prop-types';

const filter = createFilterOptions()

export const CompanySearch = (props) => {
  return (
    <Box sx={{ width: "100%", display: 'flex', alignItems: 'flex-end' }}>
      {/* {props.children} */}
      {props.icon && props.icon}
      <Autocomplete
        value={props.value}
        onChange={(event, newValue) => {
          if (typeof newValue === 'string') {
            props.setValue({
              name: newValue,
            });
          } else if (newValue && newValue.inputValue) {
            // Create a new value from the user input
            props.setValue({
              name: newValue.inputValue,
            });
          } else {
            props.setValue(newValue);
          }
        }}
        filterOptions={(options, params) => {
          const filtered = filter(options, params);
  
          const { inputValue } = params;
          // Suggest the creation of a new value
          const isExisting = options.some((option) => inputValue === option.name);
          if (inputValue !== '' && !isExisting) {
            filtered.push({
              inputValue,
              name: `Add "${inputValue}"`,
            });
          }
  
          return filtered;
        }}
        selectOnFocus
        clearOnBlur
        handleHomeEndKeys
        id={props.id}
        options={props.options}
        getOptionLabel={(option) => {
          // Value selected with enter, right from the input
          if (typeof option === 'string') {
            return option;
          }
          // Add "xxx" option created dynamically
          if (option.inputValue) {
            return option.inputValue;
          }
          // Regular option
          return option.name;
        }}
        renderOption={(props, option) => <li {...props}>{option.name}</li>}
        sx={props.sx ? props.sx : { width: "100%" }}
        freeSolo
        renderInput={(params) => (
          <TextField
            {...params}
            label={props.label}
            variant="standard"
          />
        )}
      />
    </Box>
  )
}

CompanySearch.propTypes = {
  id: PropTypes.string,
  sx: PropTypes.any,
  forcePopupIcon: PropTypes.bool,
  options: PropTypes.array,
  noOptionsText: PropTypes.string,
  value: PropTypes.any,
  setValue: PropTypes.func,
  inputValue: PropTypes.any,
  setInputValue: PropTypes.func,
  label: PropTypes.string,
  icon: PropTypes.any
};