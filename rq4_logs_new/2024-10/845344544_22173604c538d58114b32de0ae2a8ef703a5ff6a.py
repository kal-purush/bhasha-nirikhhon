from app.domain_types.enums.event_categories import EventCategory
from app.domain_types.enums.event_subjects import EventSubject
from app.domain_types.enums.event_types import EventType
from app.domain_types.schemas.data_sync import DataSyncSearchFilter
from app.modules.data_sync.connectors import get_reancare_db_connector
from app.modules.data_sync.data_synchronizer import DataSynchronizer
import mysql.connector

#######################################################

class UserAccountEventSynchronizer:
    @staticmethod
    def get_reancare_user_create_events(filters: DataSyncSearchFilter):
        try:
            selection_condition = f"AND user.CreatedAt between '{filters.StartDate}' AND '{filters.EndDate}'" if filters is not None else ''
            rean_db_connector = get_reancare_db_connector()
            query = f"""
            SELECT
                user.id as UserId,
                user.PersonId,
                user.RoleId,
                user.TenantId,
                user.LastLogin,
                user.DefaultTimeZone,
                user.CurrentTimeZone,
                user.IsTestUser,
                user.CreatedAt,
                user.UpdatedAt,
                user.DeletedAt,
                user.CreatedAt as UserRegistrationDate
            from users as user
            WHERE
                user.IsTestUser = 0
                {selection_condition}
            """
            rows = rean_db_connector.execute_read_query(query)
            return rows
        except Exception as error:
            print("Error retrieving User Create Events:", error)
            return None

    @staticmethod
    def add_analytics_user_create_event(user):
        try:
            event_name = EventType.UserCreate.value
            event_category = EventCategory.UserAccount.value
            event_subject = EventSubject.UserAccount.value

            # user = DataSynchronizer.get_user(medication['UserId'])
            # if not user:
            #     print(f"User not found for the event: {medication}")
            #     return None

            attributes = {
                'RoleId': user['RoleId'],
                'LastLogin': user['LastLogin'],
                'DefaultTimeZone': user['DefaultTimeZone'],
                'CurrentTimeZone': user['CurrentTimeZone']
            }
            user = {
                'UserId': user['UserId'],
                'TenantId': user['TenantId'],
                'SessionId': None,
                'ResourceId': user['UserId'],
                'ResourceType': "user-account",
                'SourceName': "ReanCare",
                'SourceVersion': "Unknown",
                'EventName': event_name,
                'EventSubject': event_subject,
                'EventCategory': event_category,
                'ActionType': "user-action",
                'ActionStatement': "User created.",
                'Attributes': str(attributes),
                'Timestamp': user['CreatedAt'],
                'UserRegistrationDate': user['UserRegistrationDate']
            }
            new_event_added = DataSynchronizer.add_event(user)
            if not new_event_added:
                print(f"Not inserted data.")
                return None
            else:
                return new_event_added
        except mysql.connector.Error as error:
            print(f"Failed to insert records: {error}")
            return None

    @staticmethod
    def sync_user_create_events(filters: DataSyncSearchFilter):
        try:
            existing_event_count = 0
            synched_event_count = 0
            event_not_synched = []
            users = UserAccountEventSynchronizer.get_reancare_user_create_events(filters)
            if users:
                for user in users:
                    existing_event = DataSynchronizer.get_existing_event(
                        user['UserId'], user['UserId'], EventType.UserCreate)
                    if existing_event is not None:
                        existing_event_count += 1
                    else:
                        new_event = UserAccountEventSynchronizer.add_analytics_user_create_event(user) #add the event in event table of user analytics
                        if new_event:
                            synched_event_count += 1
                        else:
                            event_not_synched.append(user)
                print(f"Existing Event Count: {existing_event_count}")
                print(f"Synched Event Count: {synched_event_count}")
                print(f"Event Not Synched: {event_not_synched}")
            else:
                print(f"No User  Create Events found.")
        except Exception as error:
            print(f"Error syncing User Create Events: {error}")

    @staticmethod
    def get_reancare_user_delete_events(filters: DataSyncSearchFilter):
        try:
            selection_condition = f"AND user.DeletedAt between '{filters.StartDate}' AND '{filters.EndDate}'" if filters is not None else ''
            rean_db_connector = get_reancare_db_connector()
            query = f"""
            SELECT
                user.id as UserId,
                user.PersonId,
                user.RoleId,
                user.TenantId,
                user.LastLogin,
                user.DefaultTimeZone,
                user.CurrentTimeZone,
                user.IsTestUser,
                user.CreatedAt,
                user.UpdatedAt,
                user.DeletedAt,
                user.CreatedAt as UserRegistrationDate
            from users as user
            WHERE
                user.IsTestUser = 0
                AND
                user.DeletedAt IS NOT NULL
                {selection_condition}
            """
            rows = rean_db_connector.execute_read_query(query)
            return rows
        except Exception as error:
            print("Error retrieving User Delete Events:", error)
            return None

    @staticmethod
    def add_analytics_user_delete_event(user):
        try:
            event_name = EventType.UserDelete.value
            event_category = EventCategory.UserAccount.value
            event_subject = EventSubject.UserAccount.value

            # user = DataSynchronizer.get_user(medication['UserId'])
            # if not user:
            #     print(f"User not found for the event: {medication}")
            #     return None
            attributes = {
                'RoleId': user['RoleId'],
                'LastLogin': user['LastLogin'],
                'DefaultTimeZone': user['DefaultTimeZone'],
                'CurrentTimeZone': user['CurrentTimeZone']
            }
            user = {
                'UserId': user['UserId'],
                'TenantId': user['TenantId'],
                'SessionId': None,
                'ResourceId': user['UserId'],
                'ResourceType': "user-account",
                'SourceName': "ReanCare",
                'SourceVersion': "Unknown",
                'EventName': event_name,
                'EventSubject': event_subject,
                'EventCategory': event_category,
                'ActionType': "user-action",
                'ActionStatement': "User deleted.",
                'Attributes': str(attributes),
                'Timestamp': user['DeletedAt'],
                'UserRegistrationDate': user['UserRegistrationDate']
            }
            new_event_added = DataSynchronizer.add_event(user)
            if not new_event_added:
                print(f"Not inserted data.")
                return None
            else:
                return new_event_added
        except mysql.connector.Error as error:
            print(f"Failed to insert event: {error}")
            return None

    @staticmethod
    def sync_user_delete_events(filters: DataSyncSearchFilter):
        try:
            existing_event_count = 0
            synched_event_count = 0
            event_not_synched = []
            deleted_users = UserAccountEventSynchronizer.get_reancare_user_delete_events(filters)
            if deleted_users:
                for deleted_user in deleted_users:
                    existing_event = DataSynchronizer.get_existing_event(
                        deleted_user['UserId'], deleted_user['UserId'], EventType.UserDelete)
                    if existing_event is not None:
                        existing_event_count += 1
                    else:
                        new_event = UserAccountEventSynchronizer.add_analytics_user_delete_event(deleted_user)
                        if new_event:
                            synched_event_count += 1
                        else:
                            event_not_synched.append(deleted_user)
                print(f"Existing Event Count: {existing_event_count}")
                print(f"Synched Event Count: {synched_event_count}")
                print(f"Event Not Synched: {event_not_synched}")
            else:
                print(f"No User Delete Events found.")
        except Exception as error:
            print(f"Error syncing User Delete Events: {error}")

    @staticmethod
    def get_reancare_user_password_reset_code_events(filters: DataSyncSearchFilter):
        try:
            
            selection_condition = f"otp.CreatedAt between '{filters.StartDate}' AND '{filters.EndDate}'" if filters is not None else ''

            rean_db_connector = get_reancare_db_connector()
            query = f"""
            SELECT
                otp.id, 
                otp.UserId,
                otp.Purpose,
                otp.ValidFrom,
                otp.ValidTill,
                otp.CreatedAt,
                otp.UpdatedAt,
                otp.DeletedAt,
                user.id as UserId,
                user.TenantId as TenantId,
                user.CreatedAt as UserRegistrationDate
            FROM otp as otp
            JOIN users user ON user.id = otp.UserId
            WHERE
                user.IsTestUser = 0
                AND
                otp.Purpose = 'PasswordReset'
                {selection_condition}
            """
            rows = rean_db_connector.execute_read_query(query)
            return rows
        except Exception as error:
            print("Error retrieving user password reset Events:", error)
            return None
        
    @staticmethod
    def add_user_password_reset_code_events(reset_code):
        try:
            event_name = EventType.UserSendPasswordResetCode.value
            event_category = EventCategory.UserAccount.value
            event_subject = EventSubject.UserAccount.value

            attributes = {
                'Purpose': reset_code['Purpose'],
                'ValidTill': reset_code['ValidTill'],
                'ValidFrom': reset_code['ValidFrom'],
            }
            reset_code = {
                'UserId': reset_code['UserId'],
                'TenantId': reset_code['TenantId'],
                'SessionId': None,
                'ResourceId': reset_code['id'],
                'ResourceType': "user-account",
                'SourceName': "ReanCare",
                'SourceVersion': "Unknown",
                'EventName': event_name,
                'EventSubject': event_subject,
                'EventCategory': event_category,
                'ActionType': "user-action",
                'ActionStatement': "User password reset code.",
                'Attributes': str(attributes),
                'Timestamp': reset_code['CreatedAt'],
                'UserRegistrationDate': reset_code['UserRegistrationDate']
            }
            new_event_added = DataSynchronizer.add_event(reset_code)
            if not new_event_added:
                print(f"Not inserted data.")
                return None
            else:
                return new_event_added
        except mysql.connector.Error as error:
            print(f"Failed to insert records: {error}")
            return None
        
    @staticmethod
    def sync_user_password_reset_code_events(filters: DataSyncSearchFilter):
        try:
            existing_event_count = 0
            synched_event_count = 0
            event_not_synched = []
            reset_codes = UserAccountEventSynchronizer.get_reancare_user_password_reset_code_events(filters)
            if reset_codes:
                for reset_code in reset_codes:
                    existing_event = DataSynchronizer.get_existing_event(
                        reset_code['UserId'], reset_code['id'], EventType.UserSendPasswordResetCode)
                    if existing_event is not None:
                        existing_event_count += 1
                    else:
                        new_event = UserAccountEventSynchronizer.add_user_password_reset_code_events(reset_code)
                        if new_event:
                            synched_event_count += 1
                        else:
                            event_not_synched.append(reset_code)
                print(f"Existing Event Count: {existing_event_count}")
                print(f"Synched Event Count: {synched_event_count}")
                print(f"Event Not Synched: {event_not_synched}")
            else:
                print(f"No User password reset code Events found.")
        except Exception as error:
            print(f"Error syncing User password reset code Events: {error}")

    