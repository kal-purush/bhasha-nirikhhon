import { db } from "./db";
import { Response } from "express";
import { FirestoreCourseDoc } from "./types";

export const connectGroupConfig = (
	email: string,
	configName: string,
	courseId: string
) => {
	return Promise.all([
		getConfigRefByEmail(email, configName),
		getCourseRefByDocId(courseId),
	])
		.then(([configRef, courseRef]) => {
			courseRef.update({ config: configRef.id });
		})
		.catch((error) => {
			console.log(error);
			return;
		});
};

export const getConfigRefByEmail = async (
	email: string,
	configName: string
) => {
	return db
		.collection("userdata")
		.where("email", "==", email)
		.limit(1)
		.get()
		.then((snapshot) => {
			if (snapshot.empty) {
				throw new Error(`User ${email} does not exist!`);
			}
			return snapshot.docs[0].ref;
		})
		.then((user) => {
			return user.collection("config").doc(configName).get();
		})
		.then((snapshot) => {
			if (!snapshot.exists) {
				throw new Error(
					`Config ${configName} does not exist under user ${email}!`
				);
			}
			return snapshot.ref;
		});
};

export const getCourseRefByDocId = async (courseId: string) => {
	return db
		.collection("course")
		.doc(courseId)
		.get()
		.then((snapshot) => {
			if (!snapshot.exists) {
				throw new Error(`Course ${courseId} does not exist!`);
			}
			return snapshot.ref;
		});
};

export const getUserRefByEmail = async (email: string) => {
	const user = await db
		.collection("userdata")
		.where("email", "==", email)
		.limit(1)
		.get()
		.then((snapshot) => {
			if (snapshot.empty) {
				throw new Error(`User ${email} does not exist!`);
			} else {
				const docRef = snapshot.docs[0].ref;
				return docRef;
			}
		});
	return user;
};

export const deleteCollectionByRef = async (
	collectionRef: FirebaseFirestore.CollectionReference,
	batchSize: number = 50
) => {
	const query = collectionRef.orderBy("__name__").limit(batchSize);

	return new Promise((resolve, reject) => {
		deleteQueryBatch(query, resolve).catch(reject);
	});
};

export const deleteQueryBatch = async (
	query: FirebaseFirestore.Query,
	resolve: any
) => {
	const snapshot = await query.get();

	const batchSize = snapshot.size;
	if (batchSize === 0) {
		// When there are no documents left, we are done
		resolve();
		return;
	}

	// Delete documents in a batch
	const batch = db.batch();
	snapshot.docs.forEach((doc) => {
		batch.delete(doc.ref);
	});
	await batch.commit();

	// Recurse on the next process tick, to avoid
	// exploding the stack.
	process.nextTick(() => {
		deleteQueryBatch(query, resolve);
	});
};

export const internalError = (res: Response, err: any) => {
	res.status(500).send(err.toString());
};

export const getDocsDataByColRef = async (
	colRef: FirebaseFirestore.CollectionReference
) => {
	const docRefList = await colRef.listDocuments();
	const allDocsData = await Promise.all(
		docRefList.map(async (docRef) => {
			const docSnapshot = await docRef.get();
			const docData = docSnapshot.data();
			return docData;
		})
	);
	return allDocsData;
};

export const getBasicCourseById = async (courseId: string) => {
	const courseDocRef = await getCourseRefByDocId(courseId);
	const courseDocSnapshot = await courseDocRef.get();

	const {
		name,
		creator,
		dueDate,
		minGroupSize,
	} = courseDocSnapshot.data() as FirestoreCourseDoc;

	const dueDateStr = (dueDate as FirebaseFirestore.Timestamp)
		.toDate()
		.toDateString();

	return { name, creator, dueDateStr, minGroupSize, courseId };
};