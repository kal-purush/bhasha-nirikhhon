import React from 'react';
import styles from './Loading.module.css';

const Loading = () => {
	return (
		<div className={styles.backdrop}>
			<div className={styles.ldsRing}>
				<div></div>
				<div></div>
				<div></div>
				<div></div>
			</div>
		</div>
	);
};

export default Loading;