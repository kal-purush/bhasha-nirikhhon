import {readCSV} from "../deps.ts";
import KnnField from "./models/KnnField.ts";

export default async function CsvToKnnField(path:string) {
	let array = await insertCsv(path)
	let [dimensions, cords] = prepArray(array)
	const knn = new KnnField(dimensions)
	knn.addData(cords)
	return knn
}

function prepArray(array: string[][]):[string[],number[][]] {
	let headers = array.slice(0, 1)[0]
	let body = array.slice(1)
	let cords:number[][] = []
	body.forEach((row:string[])=>{
		let rowArray:number[] = []
		row.forEach((cell:string)=>{
			rowArray.push(+cell);
		})
		cords.push(rowArray)
	})
	return [headers, cords]


}

async function insertCsv(path:string) {
	const f = await Deno.open(path);
	const res: string[][] = []
	for await (const row of readCSV(f)) {
		const stringRow: string[] = []
		for await (const cell of row) {
			stringRow.push(cell.replace(/\r?\n|\r/, ""))
		}
		res.push(stringRow)
	}

	f.close();
	return res
}


