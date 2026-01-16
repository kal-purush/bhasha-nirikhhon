import { Injectable } from "@nestjs/common";
import { diff } from "just-diff";
import get from "just-safe-get";
import { EnumAuditActionTypes } from "@/constants";
import { IAuditModel } from "@/db/models/AuditModel";
import { IAuditDiffViewModel, IAuditViewModel } from "@/viewModels/audit.viewmodel";

@Injectable()
export class AuditsMapper {
	entityToViewModel({ user_id, id, created_at, entity, value_previous = "", value_current = "", action }: IAuditModel): IAuditViewModel {
		const valuePrevious = value_previous ? JSON.parse(value_previous) : {};
		const valueCurrent = value_current ? JSON.parse(value_current) : {};
		const changes = diff(valuePrevious, valueCurrent);
		const output = changes.map((change) => {
			const field = change.path.join(".");
			let action: EnumAuditActionTypes;
			if (change.op === "add") {
				action = EnumAuditActionTypes.created;
			}
			else if (change.op === "replace") {
				action = EnumAuditActionTypes.updated;
			}
			else {
				action = EnumAuditActionTypes.deleted;
			}
			return {
				action,
				field,
				valueCurrent: change.value,
				valuePrevious: get(valuePrevious, field),
			} as IAuditDiffViewModel;
		});

		return {
			id,
			action,
			entity,
			userId: user_id,
			dateCreated: created_at.getTime(),
			diff: output,
		};
	}
}