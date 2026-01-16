import { Request, Response } from "express";
import { getCourseRefByDocId } from "../../../common/utils";

export const moveGroup = async (req: Request, res: Response) => {
	const courseId = req.params.courseId;
	const { studentId, baseGroupId, destGroupId } = req.body;

	const courseDocRef = (await getCourseRefByDocId(courseId).catch((err) => {
		return res.status(404).send(err.toString());
	})) as FirebaseFirestore.DocumentReference;
	courseDocRef.id;

	const groupColRef = courseDocRef.collection("group");
	const baseStudentSnapshot = await groupColRef
		.doc(baseGroupId.toString())
		.collection("members")
		.doc(studentId)
		.get();

	if (!baseStudentSnapshot.exists) {
		return res
			.status(400)
			.send(`Student ${studentId} does not exist on group ${baseGroupId}`);
	}

	// eslint-disable-next-line max-len
	const studentData = baseStudentSnapshot.data() as FirebaseFirestore.DocumentData;
	const destStudentDocRef = groupColRef
		.doc(destGroupId.toString())
		.collection("members")
		.doc(studentId);
	await destStudentDocRef.create(studentData);
	await baseStudentSnapshot.ref.delete();

	return res.status(200).send();
};