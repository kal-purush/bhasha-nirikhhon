import mongoose from "mongoose";
import { InstanceSettings } from "./Settings";

const instanceSettingsSchema = new mongoose.Schema<InstanceSettings>({
	instanceId: String,
	triggerRegExp: String
});

const InstanceSettingsModel = mongoose.model<InstanceSettings>("InstanceSettings", instanceSettingsSchema);

// ###

const findOrCreate = async (instanceId: string): Promise<InstanceSettings> => {
	return new Promise((resolve, reject) => {
		InstanceSettingsModel.findOneAndUpdate(
			{instanceId},
			{},
			{
				returnDocument: "after",
				lean: true,
				upsert: true
			}
		).maxTimeMS(5000).exec((error, json) => {
			if (error) {
				return reject(error);
			}

			if (!json) {
				return reject(new Error("No document was returned!"));
			}

			return resolve({
				instanceId: json.instanceId,
				triggerRegExp: json.triggerRegExp ? new RegExp(json.triggerRegExp, "i") : undefined
			});
		});
	});
};

const update = async (update: InstanceSettings): Promise<InstanceSettings> => {
	const actualUpdate: any = {...update};
	if (update.triggerRegExp) {
		actualUpdate.triggerRegExp = update.triggerRegExp.source;
	}

	return new Promise((resolve, reject) => {
		InstanceSettingsModel.findOneAndUpdate(
			{instanceId: update.instanceId},
			actualUpdate,
			{
				returnDocument: "after",
				lean: true,
			}
		).maxTimeMS(5000).exec((error, json) => {
			if (error) {
				return reject(error);
			}

			if (!json) {
				return reject(new Error("No document was returned!"));
			}

			return resolve({
				instanceId: json.instanceId,
				triggerRegExp: json.triggerRegExp ? new RegExp(json.triggerRegExp, "i") : undefined
			});
		});
	});
};

export { findOrCreate, update };