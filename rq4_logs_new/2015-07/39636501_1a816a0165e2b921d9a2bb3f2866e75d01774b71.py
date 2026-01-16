"""Contains AJAX handler and functions for reading and writing posts."""
import base_handler
import remark_datastore

from google.appengine.api import users


class Handler(base_handler.Handler):
  """Handles asynchronous client requests."""
  _POST_REMARK_KEY = 'post'
  _READ_REMARKS_KEY = 'read'

  def post(self):
    # Read the action key provided by the client.
    action = self.request.get('action', '')

    # If the client wants to post...
    if action == self._POST_REMARK_KEY:
      # ... get the remark from the client request...
      remark = self.request.get('remark', None)

      if not remark:
        return

      # ... then write the remark to the datastore.
      remark_datastore.PostRemark(users.get_current_user().email(), remark)

    # If the client wants to read remarks...
    elif action == self._READ_REMARKS_KEY:
      # ... write them back out as a response.
      self.WriteTemplate(
          'remarks.html',
          {
              'remarks': remark_datastore.ReadRemarks(
                  users.get_current_user().user_id())
          })
    else:
      self.error('Invalid action key.')