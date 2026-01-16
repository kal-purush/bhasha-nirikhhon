import chalk from "chalk";
import dotenv from "dotenv";
dotenv.config();

const prenom1 = process.env.PRENOM1;
const prenom2 = process.env.PRENOM2;
const prenom3 = process.env.PRENOM3;
const prenom4 = process.env.PRENOM4;

console.log(
	chalk.blue(`${prenom1}`) +
		chalk.red(`${prenom2}`) +
		chalk.grey(`${prenom3}`) +
		chalk.yellow(`${prenom4}`),
);