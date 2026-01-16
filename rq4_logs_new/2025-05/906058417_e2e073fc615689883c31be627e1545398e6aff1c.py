from ..attribute import Attribute
from ..fbs import Notification
import time



class NotificationPack:
    def __init__(self, notifications: list =None):
        self.notifications = notifications

    def is_empty(self):
        """
        Check if the notification list is empty.
        """
        return len(self.notifications) == 0

    def has_alert(self):
        """
        Check if any notification in the list has an alert.
        """
        for notification in self.notifications:
            if notification.Type() == 1: # Assuming 1 indicates an alert, 2 error
                return True
        return False



class NotificationAttribute(Attribute):
    """
    Attribute for sending boolean values (True or False).
    Ensures that only valid booleans are sent.
    """
   
    # ---

    def __init__(self, reactor, topic, mode, settings=None):
        super().__init__(reactor, topic, mode, settings)
    
    # ---

    def on_att_message(self, data):
        """
        Callback for handling incoming messages.
        - Updates the value and triggers the update event.
        """
        obj = Notification.Notification.GetRootAsNotification(data, 0)

        self.queue.append(obj)

        super().on_message_top(obj)
    

    # ---

    def pop_all(self):
        """
        Pop all messages from the queue.
        """
        notifications = []
        while len(self.queue) > 0:
            notifications.append(self.pop())
        return NotificationPack(notifications)
