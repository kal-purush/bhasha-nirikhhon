import React from 'react';
import { NavLink } from 'react-router-dom';
import { getResponsiveClasses } from '../../utils/utils';
import globalStyle from '../../styles/global.css';

import style from './Table.css';

const Table = ({
	children
}) => {

	const classes = [
		globalStyle.width100per,
	].join(' ');

	return (
		<div className={classes}>
			{children}
		</div>
	);
}

const TableHeadingRow = ({
	children
}) => {

	const classes = [
		style.headingRow,
	].join(' ');

	return (
		<div className={classes}>
			{children}
		</div>
	);
}

const TableHeadingData = ({
	width = "auto",
	textAlign = "left",
	children
}) => {

	const classes = [
		getResponsiveClasses('flexWidth', width),
		getResponsiveClasses('textAlign', textAlign),
		style.heading,
	].join(' ');

	return (
		<div className={classes}>
			{children}
		</div>
	);
}

const TableRow = ({
	to = false,
	children
}) => {

	const classes = [
		style.row,
	].join(' ');

	const classesLinks = [
		style.rowLink,
	].join(' ');

	if (!to) {
		return (
			<div className={classes}>
				{children}
			</div>
		);
	} else {
		return (
			<NavLink
				className={[classes, classesLinks].join(' ')}
				to={to}
			>
				{children}
			</NavLink>
		);
	}
}

const TableData = ({
	width = "auto", // "auto"|"grow"|"even"|"[number(per|px)]", default: "auto"
	textAlign = "left",
	display = "block",
	children
}) => {

	const classes = [
		getResponsiveClasses('flexWidth', width),
		getResponsiveClasses('textAlign', textAlign),
		getResponsiveClasses('display', display),
		style.data,
	].join(' ');

	return (
		<div className={classes}>
			{children}
		</div>
	);
}

const TableMobileHeading = ({
	children
}) => {

	const classes = [
		style.mobileHeading,
	].join(' ');

	return (
		<span className={classes}>
			{children}
		</span>
	);
}

export {
	Table,
	TableHeadingRow,
	TableHeadingData,
	TableRow,
	TableData,
	TableMobileHeading,
};