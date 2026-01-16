import { FirestoreUserDoc } from "../../common/types";
import { db } from "../../common/db";
import { Request, Response } from "express";
import { getUserRefByEmail, getBasicCourseById } from "../../common/utils";
import { defaultConfig } from "../../common/config";

export const postInstructor = async (req: Request, res: Response) => {
	// need data validation
	const { email, name } = req.body;

	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	await getUserRefByEmail(email).catch(async (err) => {
		const user: FirestoreUserDoc = {
			name,
			email,
			course: [],
		};

		const userColRef = db.collection("userdata");
		const userDocRef = await userColRef.add(user);
		await userDocRef.collection("config").doc("default").set(defaultConfig);
		return res.status(201).send(`Successfully created user with ${email}`);
	});
	return res.status(409).send(`User with email ${email} already exists`);
};

export const getInstructorCourse = async (req: Request, res: Response) => {
	const email = req.params.userId;
	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	const userDocRef = (await getUserRefByEmail(email).catch((err) => {
		return res.status(404).send(`User ${email} does not exist`);
	})) as FirebaseFirestore.DocumentReference;

	const userDocSnapshot = await userDocRef.get();
	const courseIdList: string[] = userDocSnapshot.get("course");

	const allCoursesData = await Promise.all(
		courseIdList.map((courseId) => getBasicCourseById(courseId))
	);

	return res.status(200).send(allCoursesData);
};