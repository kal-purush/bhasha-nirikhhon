import { makeStyles } from "@material-ui/core/styles";

const useStyles = makeStyles((theme) => ({
	categoriesItemContent: {
		display: 'flex',
		justifyContent: 'space-between',
		borderBottom: "2px solid #FF8302",
		padding: '10px 0',
		textTransform: 'uppercase',
	},
	categoriesItemImg: {
		height: '50px',
		width: '50px',
	},
	categoriesNoPhoto: {
		marginTop: '15px',
	},
	categoriesItemName: {
		margin: '15px 0 0 50px',
	},
	categoriesItemButtons: {
		display: 'flex',

		marginLeft: '40px',
	},
	categoriesItemButtonsItem: {
		marginLeft: '40px',
	}
}));

export default useStyles;