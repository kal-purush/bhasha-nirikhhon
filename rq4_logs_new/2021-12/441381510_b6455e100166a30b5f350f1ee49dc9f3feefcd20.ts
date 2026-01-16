
  type SearchState = {
    search: SearchData
  }
  type SearchAction = {
    type: string
    value: string
    data?: SearchData
  }
  type SearchData = {
    currentValue: string,
    history: [string]
  }
  
  
  
  type DispatchSearch = (args: SearchAction) => SearchAction
  
  