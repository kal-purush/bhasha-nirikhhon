export enum ETransferStatus {
	PROCESSING = 'processing',
	DONE = 'done',
}

export interface ITransferOfCasesProgress {
	status: ETransferStatus;
	totalQuantity?: number;
	total_quantity?: number;
	quantity: number;
	createdAt?: number;
	created_at?: number;
}

export interface ITransferTasksData {
	count: number;
	onlyQuantity: boolean;
	oldUserId: number;
	newUserIds: number[];
	ownersUsers: number[];
	responsibleUsers: number[];
	auditorsUsers: number[];
	accomplicesUsers: number[];
	templatesUsers: number[];
	templateOwnerUsers: number[];
	templateResponsibleUsers: number[];
	templateAuditorsUsers: number[];
	templateAccomplicesUsers: number[];
	accessToTemplates: number[];
}

export interface ITransferGroupsData {
	count: number;
	onlyQuantity: boolean;
	oldUserId: number;
	newUserIds: number[];
	moderators: number[];
	group_owners: number[];
	group_members: number[];
	on_participants: boolean;
}

export interface ITransferActivitiesData {
	count: number;
	onlyQuantity: boolean;
	old_user_id: number;
	new_user_ids: number[];
	responsible_users: number[];
	participants_users: number[];
	calendar_sync: boolean;
}

export interface ITransferEntitiesData {
	count: number;
	onlyQuantity: boolean;
	old_user_id: number;
	new_user_ids: number[];
	call_user_ids: number[];
	entities_user_ids: {
		[key: string]: number[];
	};
	counters: {
		[key: string]: number;
	};
}