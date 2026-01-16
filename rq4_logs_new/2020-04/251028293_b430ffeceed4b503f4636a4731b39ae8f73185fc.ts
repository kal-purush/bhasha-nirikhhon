export type Maybe<T> = T | null;
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: string;
  String: string;
  Boolean: boolean;
  Int: number;
  Float: number;
  ObjectId: any;
};

export type Connected_Service = {
   __typename?: 'Connected_service';
  _id?: Maybe<Scalars['ObjectId']>;
  bakalari?: Maybe<Scalars['Boolean']>;
  edmodo?: Maybe<Scalars['Boolean']>;
  facebook?: Maybe<Scalars['Boolean']>;
  googleClassroom?: Maybe<Scalars['Boolean']>;
  user_id?: Maybe<Scalars['String']>;
};

export type Connected_ServiceInsertInput = {
  edmodo?: Maybe<Scalars['Boolean']>;
  googleClassroom?: Maybe<Scalars['Boolean']>;
  _id?: Maybe<Scalars['ObjectId']>;
  user_id?: Maybe<Scalars['String']>;
  bakalari?: Maybe<Scalars['Boolean']>;
  facebook?: Maybe<Scalars['Boolean']>;
};

export type Connected_ServiceQueryInput = {
  user_id_lte?: Maybe<Scalars['String']>;
  edmodo?: Maybe<Scalars['Boolean']>;
  _id_exists?: Maybe<Scalars['Boolean']>;
  _id_gt?: Maybe<Scalars['ObjectId']>;
  _id_nin?: Maybe<Array<Maybe<Scalars['ObjectId']>>>;
  bakalari_ne?: Maybe<Scalars['Boolean']>;
  AND?: Maybe<Array<Connected_ServiceQueryInput>>;
  googleClassroom_exists?: Maybe<Scalars['Boolean']>;
  _id_gte?: Maybe<Scalars['ObjectId']>;
  user_id_gte?: Maybe<Scalars['String']>;
  facebook_exists?: Maybe<Scalars['Boolean']>;
  _id_ne?: Maybe<Scalars['ObjectId']>;
  _id_lt?: Maybe<Scalars['ObjectId']>;
  user_id_in?: Maybe<Array<Maybe<Scalars['String']>>>;
  _id?: Maybe<Scalars['ObjectId']>;
  bakalari_exists?: Maybe<Scalars['Boolean']>;
  user_id_gt?: Maybe<Scalars['String']>;
  bakalari?: Maybe<Scalars['Boolean']>;
  googleClassroom_ne?: Maybe<Scalars['Boolean']>;
  user_id_nin?: Maybe<Array<Maybe<Scalars['String']>>>;
  _id_in?: Maybe<Array<Maybe<Scalars['ObjectId']>>>;
  facebook?: Maybe<Scalars['Boolean']>;
  user_id_ne?: Maybe<Scalars['String']>;
  user_id?: Maybe<Scalars['String']>;
  user_id_exists?: Maybe<Scalars['Boolean']>;
  OR?: Maybe<Array<Connected_ServiceQueryInput>>;
  edmodo_exists?: Maybe<Scalars['Boolean']>;
  user_id_lt?: Maybe<Scalars['String']>;
  _id_lte?: Maybe<Scalars['ObjectId']>;
  facebook_ne?: Maybe<Scalars['Boolean']>;
  edmodo_ne?: Maybe<Scalars['Boolean']>;
  googleClassroom?: Maybe<Scalars['Boolean']>;
};

export enum Connected_ServiceSortByInput {
  IdAsc = '_ID_ASC',
  IdDesc = '_ID_DESC',
  UserIdAsc = 'USER_ID_ASC',
  UserIdDesc = 'USER_ID_DESC'
}

export type Connected_ServiceUpdateInput = {
  user_id?: Maybe<Scalars['String']>;
  bakalari?: Maybe<Scalars['Boolean']>;
  facebook?: Maybe<Scalars['Boolean']>;
  edmodo?: Maybe<Scalars['Boolean']>;
  googleClassroom?: Maybe<Scalars['Boolean']>;
  _id?: Maybe<Scalars['ObjectId']>;
};

export type DeleteManyPayload = {
   __typename?: 'DeleteManyPayload';
  deletedCount: Scalars['Int'];
};

export type InsertManyPayload = {
   __typename?: 'InsertManyPayload';
  insertedIds: Array<Maybe<Scalars['ObjectId']>>;
};

export type Mutation = {
   __typename?: 'Mutation';
  deleteManyConnected_services?: Maybe<DeleteManyPayload>;
  deleteManySettings?: Maybe<DeleteManyPayload>;
  deleteManyTasks?: Maybe<DeleteManyPayload>;
  deleteManyUsers?: Maybe<DeleteManyPayload>;
  deleteOneConnected_service?: Maybe<Connected_Service>;
  deleteOneSetting?: Maybe<Setting>;
  deleteOneTask?: Maybe<Task>;
  deleteOneUser?: Maybe<User>;
  insertManyConnected_services?: Maybe<InsertManyPayload>;
  insertManySettings?: Maybe<InsertManyPayload>;
  insertManyTasks?: Maybe<InsertManyPayload>;
  insertManyUsers?: Maybe<InsertManyPayload>;
  insertOneConnected_service?: Maybe<Connected_Service>;
  insertOneSetting?: Maybe<Setting>;
  insertOneTask?: Maybe<Task>;
  insertOneUser?: Maybe<User>;
  replaceOneConnected_service?: Maybe<Connected_Service>;
  replaceOneSetting?: Maybe<Setting>;
  replaceOneTask?: Maybe<Task>;
  replaceOneUser?: Maybe<User>;
  updateManyConnected_services?: Maybe<UpdateManyPayload>;
  updateManySettings?: Maybe<UpdateManyPayload>;
  updateManyTasks?: Maybe<UpdateManyPayload>;
  updateManyUsers?: Maybe<UpdateManyPayload>;
  updateOneConnected_service?: Maybe<Connected_Service>;
  updateOneSetting?: Maybe<Setting>;
  updateOneTask?: Maybe<Task>;
  updateOneUser?: Maybe<User>;
  upsertOneConnected_service?: Maybe<Connected_Service>;
  upsertOneSetting?: Maybe<Setting>;
  upsertOneTask?: Maybe<Task>;
  upsertOneUser?: Maybe<User>;
};


export type MutationDeleteManyConnected_ServicesArgs = {
  query?: Maybe<Connected_ServiceQueryInput>;
};


export type MutationDeleteManySettingsArgs = {
  query?: Maybe<SettingQueryInput>;
};


export type MutationDeleteManyTasksArgs = {
  query?: Maybe<TaskQueryInput>;
};


export type MutationDeleteManyUsersArgs = {
  query?: Maybe<UserQueryInput>;
};


export type MutationDeleteOneConnected_ServiceArgs = {
  query: Connected_ServiceQueryInput;
};


export type MutationDeleteOneSettingArgs = {
  query: SettingQueryInput;
};


export type MutationDeleteOneTaskArgs = {
  query: TaskQueryInput;
};


export type MutationDeleteOneUserArgs = {
  query: UserQueryInput;
};


export type MutationInsertManyConnected_ServicesArgs = {
  data: Array<Connected_ServiceInsertInput>;
};


export type MutationInsertManySettingsArgs = {
  data: Array<SettingInsertInput>;
};


export type MutationInsertManyTasksArgs = {
  data: Array<TaskInsertInput>;
};


export type MutationInsertManyUsersArgs = {
  data: Array<UserInsertInput>;
};


export type MutationInsertOneConnected_ServiceArgs = {
  data: Connected_ServiceInsertInput;
};


export type MutationInsertOneSettingArgs = {
  data: SettingInsertInput;
};


export type MutationInsertOneTaskArgs = {
  data: TaskInsertInput;
};


export type MutationInsertOneUserArgs = {
  data: UserInsertInput;
};


export type MutationReplaceOneConnected_ServiceArgs = {
  query?: Maybe<Connected_ServiceQueryInput>;
  data: Connected_ServiceInsertInput;
};


export type MutationReplaceOneSettingArgs = {
  query?: Maybe<SettingQueryInput>;
  data: SettingInsertInput;
};


export type MutationReplaceOneTaskArgs = {
  query?: Maybe<TaskQueryInput>;
  data: TaskInsertInput;
};


export type MutationReplaceOneUserArgs = {
  query?: Maybe<UserQueryInput>;
  data: UserInsertInput;
};


export type MutationUpdateManyConnected_ServicesArgs = {
  query?: Maybe<Connected_ServiceQueryInput>;
  set: Connected_ServiceUpdateInput;
};


export type MutationUpdateManySettingsArgs = {
  query?: Maybe<SettingQueryInput>;
  set: SettingUpdateInput;
};


export type MutationUpdateManyTasksArgs = {
  query?: Maybe<TaskQueryInput>;
  set: TaskUpdateInput;
};


export type MutationUpdateManyUsersArgs = {
  query?: Maybe<UserQueryInput>;
  set: UserUpdateInput;
};


export type MutationUpdateOneConnected_ServiceArgs = {
  query?: Maybe<Connected_ServiceQueryInput>;
  set: Connected_ServiceUpdateInput;
};


export type MutationUpdateOneSettingArgs = {
  set: SettingUpdateInput;
  query?: Maybe<SettingQueryInput>;
};


export type MutationUpdateOneTaskArgs = {
  query?: Maybe<TaskQueryInput>;
  set: TaskUpdateInput;
};


export type MutationUpdateOneUserArgs = {
  query?: Maybe<UserQueryInput>;
  set: UserUpdateInput;
};


export type MutationUpsertOneConnected_ServiceArgs = {
  query?: Maybe<Connected_ServiceQueryInput>;
  data: Connected_ServiceInsertInput;
};


export type MutationUpsertOneSettingArgs = {
  data: SettingInsertInput;
  query?: Maybe<SettingQueryInput>;
};


export type MutationUpsertOneTaskArgs = {
  query?: Maybe<TaskQueryInput>;
  data: TaskInsertInput;
};


export type MutationUpsertOneUserArgs = {
  data: UserInsertInput;
  query?: Maybe<UserQueryInput>;
};


export type Query = {
   __typename?: 'Query';
  connected_service?: Maybe<Connected_Service>;
  connected_services: Array<Maybe<Connected_Service>>;
  setting?: Maybe<Setting>;
  settings: Array<Maybe<Setting>>;
  task?: Maybe<Task>;
  tasks: Array<Maybe<Task>>;
  user?: Maybe<User>;
  users: Array<Maybe<User>>;
};


export type QueryConnected_ServiceArgs = {
  query?: Maybe<Connected_ServiceQueryInput>;
};


export type QueryConnected_ServicesArgs = {
  query?: Maybe<Connected_ServiceQueryInput>;
  limit?: Maybe<Scalars['Int']>;
  sortBy?: Maybe<Connected_ServiceSortByInput>;
};


export type QuerySettingArgs = {
  query?: Maybe<SettingQueryInput>;
};


export type QuerySettingsArgs = {
  query?: Maybe<SettingQueryInput>;
  limit?: Maybe<Scalars['Int']>;
  sortBy?: Maybe<SettingSortByInput>;
};


export type QueryTaskArgs = {
  query?: Maybe<TaskQueryInput>;
};


export type QueryTasksArgs = {
  limit?: Maybe<Scalars['Int']>;
  sortBy?: Maybe<TaskSortByInput>;
  query?: Maybe<TaskQueryInput>;
};


export type QueryUserArgs = {
  query?: Maybe<UserQueryInput>;
};


export type QueryUsersArgs = {
  query?: Maybe<UserQueryInput>;
  limit?: Maybe<Scalars['Int']>;
  sortBy?: Maybe<UserSortByInput>;
};

export type Setting = {
   __typename?: 'Setting';
  _id?: Maybe<Scalars['ObjectId']>;
  store_credentials_locally?: Maybe<Scalars['Boolean']>;
  user_id?: Maybe<Scalars['String']>;
};

export type SettingInsertInput = {
  user_id?: Maybe<Scalars['String']>;
  store_credentials_locally?: Maybe<Scalars['Boolean']>;
  _id?: Maybe<Scalars['ObjectId']>;
};

export type SettingQueryInput = {
  OR?: Maybe<Array<SettingQueryInput>>;
  store_credentials_locally_exists?: Maybe<Scalars['Boolean']>;
  _id_ne?: Maybe<Scalars['ObjectId']>;
  user_id_gt?: Maybe<Scalars['String']>;
  _id_in?: Maybe<Array<Maybe<Scalars['ObjectId']>>>;
  _id_gt?: Maybe<Scalars['ObjectId']>;
  _id_lt?: Maybe<Scalars['ObjectId']>;
  user_id?: Maybe<Scalars['String']>;
  user_id_lt?: Maybe<Scalars['String']>;
  user_id_in?: Maybe<Array<Maybe<Scalars['String']>>>;
  store_credentials_locally?: Maybe<Scalars['Boolean']>;
  store_credentials_locally_ne?: Maybe<Scalars['Boolean']>;
  AND?: Maybe<Array<SettingQueryInput>>;
  user_id_lte?: Maybe<Scalars['String']>;
  _id_nin?: Maybe<Array<Maybe<Scalars['ObjectId']>>>;
  user_id_gte?: Maybe<Scalars['String']>;
  user_id_ne?: Maybe<Scalars['String']>;
  user_id_nin?: Maybe<Array<Maybe<Scalars['String']>>>;
  _id_gte?: Maybe<Scalars['ObjectId']>;
  user_id_exists?: Maybe<Scalars['Boolean']>;
  _id_exists?: Maybe<Scalars['Boolean']>;
  _id?: Maybe<Scalars['ObjectId']>;
  _id_lte?: Maybe<Scalars['ObjectId']>;
};

export enum SettingSortByInput {
  IdAsc = '_ID_ASC',
  IdDesc = '_ID_DESC',
  UserIdAsc = 'USER_ID_ASC',
  UserIdDesc = 'USER_ID_DESC'
}

export type SettingUpdateInput = {
  _id?: Maybe<Scalars['ObjectId']>;
  user_id?: Maybe<Scalars['String']>;
  store_credentials_locally?: Maybe<Scalars['Boolean']>;
};

export type Task = {
   __typename?: 'Task';
  _id?: Maybe<Scalars['ObjectId']>;
  body?: Maybe<Scalars['String']>;
  props?: Maybe<Scalars['String']>;
  resolved?: Maybe<Scalars['Boolean']>;
  sender?: Maybe<Scalars['String']>;
  service?: Maybe<Scalars['String']>;
  starred?: Maybe<Scalars['Boolean']>;
  user_id?: Maybe<Scalars['String']>;
};

export type TaskInsertInput = {
  user_id?: Maybe<Scalars['String']>;
  service?: Maybe<Scalars['String']>;
  sender?: Maybe<Scalars['String']>;
  body?: Maybe<Scalars['String']>;
  starred?: Maybe<Scalars['Boolean']>;
  resolved?: Maybe<Scalars['Boolean']>;
  props?: Maybe<Scalars['String']>;
  _id?: Maybe<Scalars['ObjectId']>;
};

export type TaskQueryInput = {
  user_id?: Maybe<Scalars['String']>;
  user_id_gte?: Maybe<Scalars['String']>;
  body_gt?: Maybe<Scalars['String']>;
  props_exists?: Maybe<Scalars['Boolean']>;
  starred_ne?: Maybe<Scalars['Boolean']>;
  service_gt?: Maybe<Scalars['String']>;
  service_gte?: Maybe<Scalars['String']>;
  resolved?: Maybe<Scalars['Boolean']>;
  body_ne?: Maybe<Scalars['String']>;
  _id_in?: Maybe<Array<Maybe<Scalars['ObjectId']>>>;
  props?: Maybe<Scalars['String']>;
  sender_in?: Maybe<Array<Maybe<Scalars['String']>>>;
  sender?: Maybe<Scalars['String']>;
  user_id_lt?: Maybe<Scalars['String']>;
  sender_lt?: Maybe<Scalars['String']>;
  props_gt?: Maybe<Scalars['String']>;
  sender_gt?: Maybe<Scalars['String']>;
  body_nin?: Maybe<Array<Maybe<Scalars['String']>>>;
  _id_lte?: Maybe<Scalars['ObjectId']>;
  resolved_ne?: Maybe<Scalars['Boolean']>;
  body_lt?: Maybe<Scalars['String']>;
  _id_gte?: Maybe<Scalars['ObjectId']>;
  props_ne?: Maybe<Scalars['String']>;
  _id_gt?: Maybe<Scalars['ObjectId']>;
  _id_nin?: Maybe<Array<Maybe<Scalars['ObjectId']>>>;
  sender_nin?: Maybe<Array<Maybe<Scalars['String']>>>;
  sender_ne?: Maybe<Scalars['String']>;
  props_gte?: Maybe<Scalars['String']>;
  props_nin?: Maybe<Array<Maybe<Scalars['String']>>>;
  AND?: Maybe<Array<TaskQueryInput>>;
  body_gte?: Maybe<Scalars['String']>;
  user_id_nin?: Maybe<Array<Maybe<Scalars['String']>>>;
  _id_lt?: Maybe<Scalars['ObjectId']>;
  user_id_ne?: Maybe<Scalars['String']>;
  user_id_lte?: Maybe<Scalars['String']>;
  body_lte?: Maybe<Scalars['String']>;
  service_exists?: Maybe<Scalars['Boolean']>;
  body_exists?: Maybe<Scalars['Boolean']>;
  service?: Maybe<Scalars['String']>;
  _id_ne?: Maybe<Scalars['ObjectId']>;
  body?: Maybe<Scalars['String']>;
  user_id_exists?: Maybe<Scalars['Boolean']>;
  starred?: Maybe<Scalars['Boolean']>;
  service_nin?: Maybe<Array<Maybe<Scalars['String']>>>;
  sender_gte?: Maybe<Scalars['String']>;
  _id?: Maybe<Scalars['ObjectId']>;
  props_lt?: Maybe<Scalars['String']>;
  starred_exists?: Maybe<Scalars['Boolean']>;
  sender_lte?: Maybe<Scalars['String']>;
  resolved_exists?: Maybe<Scalars['Boolean']>;
  _id_exists?: Maybe<Scalars['Boolean']>;
  OR?: Maybe<Array<TaskQueryInput>>;
  service_in?: Maybe<Array<Maybe<Scalars['String']>>>;
  props_in?: Maybe<Array<Maybe<Scalars['String']>>>;
  service_ne?: Maybe<Scalars['String']>;
  user_id_in?: Maybe<Array<Maybe<Scalars['String']>>>;
  user_id_gt?: Maybe<Scalars['String']>;
  service_lt?: Maybe<Scalars['String']>;
  sender_exists?: Maybe<Scalars['Boolean']>;
  props_lte?: Maybe<Scalars['String']>;
  body_in?: Maybe<Array<Maybe<Scalars['String']>>>;
  service_lte?: Maybe<Scalars['String']>;
};

export enum TaskSortByInput {
  PropsAsc = 'PROPS_ASC',
  IdAsc = '_ID_ASC',
  IdDesc = '_ID_DESC',
  UserIdAsc = 'USER_ID_ASC',
  ServiceAsc = 'SERVICE_ASC',
  BodyAsc = 'BODY_ASC',
  BodyDesc = 'BODY_DESC',
  PropsDesc = 'PROPS_DESC',
  UserIdDesc = 'USER_ID_DESC',
  ServiceDesc = 'SERVICE_DESC',
  SenderAsc = 'SENDER_ASC',
  SenderDesc = 'SENDER_DESC'
}

export type TaskUpdateInput = {
  starred?: Maybe<Scalars['Boolean']>;
  resolved?: Maybe<Scalars['Boolean']>;
  props?: Maybe<Scalars['String']>;
  _id?: Maybe<Scalars['ObjectId']>;
  user_id?: Maybe<Scalars['String']>;
  service?: Maybe<Scalars['String']>;
  sender?: Maybe<Scalars['String']>;
  body?: Maybe<Scalars['String']>;
};

export type UpdateManyPayload = {
   __typename?: 'UpdateManyPayload';
  matchedCount: Scalars['Int'];
  modifiedCount: Scalars['Int'];
};

export type User = {
   __typename?: 'User';
  _id?: Maybe<Scalars['ObjectId']>;
  email?: Maybe<Scalars['String']>;
  user_id?: Maybe<Scalars['String']>;
  username?: Maybe<Scalars['String']>;
};

export type UserInsertInput = {
  user_id?: Maybe<Scalars['String']>;
  username?: Maybe<Scalars['String']>;
  email?: Maybe<Scalars['String']>;
  _id?: Maybe<Scalars['ObjectId']>;
};

export type UserQueryInput = {
  username_exists?: Maybe<Scalars['Boolean']>;
  _id_gte?: Maybe<Scalars['ObjectId']>;
  username_lt?: Maybe<Scalars['String']>;
  username_gt?: Maybe<Scalars['String']>;
  user_id_lte?: Maybe<Scalars['String']>;
  username?: Maybe<Scalars['String']>;
  username_in?: Maybe<Array<Maybe<Scalars['String']>>>;
  AND?: Maybe<Array<UserQueryInput>>;
  email_ne?: Maybe<Scalars['String']>;
  username_lte?: Maybe<Scalars['String']>;
  user_id?: Maybe<Scalars['String']>;
  _id_gt?: Maybe<Scalars['ObjectId']>;
  email_gte?: Maybe<Scalars['String']>;
  OR?: Maybe<Array<UserQueryInput>>;
  email_nin?: Maybe<Array<Maybe<Scalars['String']>>>;
  _id_in?: Maybe<Array<Maybe<Scalars['ObjectId']>>>;
  email?: Maybe<Scalars['String']>;
  user_id_ne?: Maybe<Scalars['String']>;
  username_ne?: Maybe<Scalars['String']>;
  user_id_lt?: Maybe<Scalars['String']>;
  user_id_nin?: Maybe<Array<Maybe<Scalars['String']>>>;
  _id?: Maybe<Scalars['ObjectId']>;
  _id_ne?: Maybe<Scalars['ObjectId']>;
  user_id_in?: Maybe<Array<Maybe<Scalars['String']>>>;
  username_nin?: Maybe<Array<Maybe<Scalars['String']>>>;
  email_lte?: Maybe<Scalars['String']>;
  _id_lte?: Maybe<Scalars['ObjectId']>;
  _id_nin?: Maybe<Array<Maybe<Scalars['ObjectId']>>>;
  user_id_exists?: Maybe<Scalars['Boolean']>;
  user_id_gt?: Maybe<Scalars['String']>;
  email_lt?: Maybe<Scalars['String']>;
  _id_lt?: Maybe<Scalars['ObjectId']>;
  email_gt?: Maybe<Scalars['String']>;
  _id_exists?: Maybe<Scalars['Boolean']>;
  user_id_gte?: Maybe<Scalars['String']>;
  username_gte?: Maybe<Scalars['String']>;
  email_in?: Maybe<Array<Maybe<Scalars['String']>>>;
  email_exists?: Maybe<Scalars['Boolean']>;
};

export enum UserSortByInput {
  UserIdDesc = 'USER_ID_DESC',
  UsernameAsc = 'USERNAME_ASC',
  UsernameDesc = 'USERNAME_DESC',
  EmailAsc = 'EMAIL_ASC',
  EmailDesc = 'EMAIL_DESC',
  IdAsc = '_ID_ASC',
  IdDesc = '_ID_DESC',
  UserIdAsc = 'USER_ID_ASC'
}

export type UserUpdateInput = {
  username?: Maybe<Scalars['String']>;
  email?: Maybe<Scalars['String']>;
  _id?: Maybe<Scalars['ObjectId']>;
  user_id?: Maybe<Scalars['String']>;
};