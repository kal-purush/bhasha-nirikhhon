const USERS_TABLE = {
  TABLE_NAME: "users",
  ID: "id",
  NAME: "name",
  AVATAR: "avatar",
  WHATSAPP: "whatsapp",
  BIO: "bio",
};

const CLASSES_TABLE = {
  TABLE_NAME: "classes",
  ID: "id",
  SUBJECT: "subject",
  COST: "cost",
  USER_ID: "user_id",
};

const CLASSES_SCHEDULE_TABLE = {
  TABLE_NAME: "class_schedule",
  ID: "id",
  WEEK_DAY: "week_day",
  TO: "to",
  FROM: "from",
  CLASS_ID: "class_id",
};

const CONNECTIONS_TABLE = {
  TABLE_NAME: "connections",
  ID: "id",
  USER_ID: "user_id",
  CREATED_AT: "created_at",
};

export {
  USERS_TABLE,
  CLASSES_TABLE,
  CLASSES_SCHEDULE_TABLE,
  CONNECTIONS_TABLE,
};