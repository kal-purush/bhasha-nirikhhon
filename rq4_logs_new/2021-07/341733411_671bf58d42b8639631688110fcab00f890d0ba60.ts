import { SurveyQuestion } from "../../../common/types";
import { storage } from "../../../common/db";
import { Request, Response } from "express";
import { getCourseRefByDocId } from "../../../common/utils";
import { FieldInfo, parseAsync } from "json2csv";
import { v4 as uuidv4 } from "uuid";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";

export const postCourseCSV = async (req: Request, res: Response) => {
	const courseId = req.params.courseId;
	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	const courseDocRef = (await getCourseRefByDocId(courseId).catch((err) => {
		return res.status(404).send(`Course with id ${courseId} is not found`);
	})) as FirebaseFirestore.DocumentReference;

	// fetch headers for csv file
	const courseDocSnapshot = await courseDocRef.get();
	const fields: (
		| string
		| FieldInfo<FirebaseFirestore.DocumentData>
	)[] = (courseDocSnapshot.get("question") as SurveyQuestion[]).map((q) => {
		const questionHash = q.question.hash;
		return {
			label: questionHash,
			value: "surveyResponse.".concat(questionHash),
		};
	});
	fields.push("studentId", "fullName");

	// convert docs to csv
	const surveyColSnapshot = await courseDocRef.collection("survey").get();
	const surveyDocs = surveyColSnapshot.docs.map((doc) => doc.data());
	const outputCSV = await parseAsync(surveyDocs, { fields });
	const fileName = `${courseId}.csv`;
	const tempFile = path.join(os.tmpdir(), fileName);

	return new Promise<void>((resolve, reject) => {
		fs.writeFile(tempFile, outputCSV, (error) => {
			if (error) {
				reject(error);
				return;
			}
			const bucket = storage.bucket("zing-backend.appspot.com");

			bucket
				.upload(tempFile, {
					metadata: {
						metadata: {
							firebaseStorageDownloadTokens: uuidv4(),
						},
					},
				})
				.then(() => {
					res.status(200).send(
						`Successfully converted responses\
							for ${courseId} to a CSV file.`
					);
					resolve();
				});
		});
	});
};