const baseUrl = "https://api.tvmaze.com";
export const getShowDetails = (showId) => fetch(`${baseUrl}/shows/${showId}`);

export const getShowList = () => fetch(`${baseUrl}/shows`);