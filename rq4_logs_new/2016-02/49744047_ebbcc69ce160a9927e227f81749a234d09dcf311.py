"""Manage Spark Webhooks."""

from restReq import RestReq


class Hook(object):
    """Represent a Cisco Spark Webhook."""
    def __init__(self, hookId, name, resource, event, targetUrl, filt):
        self._hookId = hookId
        self._name = name
        self._resource = resource
        self._event = event
        self._targetUrl = targetUrl
        self._filter = filt

    @property
    def id(self):
        return self._hookId

    @property
    def name(self):
        return self._name

    @property
    def resource(self):
        return self._resource

    @property
    def event(self):
        return self._event

    @property
    def targetUrl(self):
        return self._targetUrl

    @property
    def filter(self):
        return self._filter


class WebHooks(object):
    """Manipulate Cisco Spark Webhooks."""
    HOOK_URL = "https://api.ciscospark.com/v1/webhooks"

    def __init__(self, token, timeout=None):
        self.restReq = RestReq(token, timeout=timeout)

    def getWebHooks(self, max=None):
        """Return list of existing webhooks as a list of Hook objects."""
        payload = {'max': max}
        response = self.restReq.get(self.HOOK_URL, payload=payload)

        if response.status_code != 200:
            response.raise_for_status()
        else:
            hook_array = []
            results = response.json()
            for r in results['items']:
                hook_array.append(
                    Hook(r.get('id'), r.get('name'), r.get('resource'), r.get('event'),
                         r.get('targetUrl'), r.get('filter')))

            return hook_array

    def getById(self, hookId):
        """Return Hook object corresponding to the ID provided."""
        response = self.restReq.get(self.HOOK_URL, id=hookId)

        if response.status_code != 200:
            response.raise_for_status()
        else:
            r = response.json()
            return Hook(r.get('id'), r.get('name'), r.get('resource'), r.get('event'),
                        r.get('targetUrl'), r.get('filter'))

    def getByName(self, name, max=None):
        """Return list of Hook objects corresponding to the name provided."""
        hook_array = self.getWebHooks(max=max)
        filtered_array = [h for h in hook_array if h.name == name]
        return filtered_array

    def getByUrl(self, url, max=None):
        """Return list of Hook objects corresponding to the url provided."""
        hook_array = self.getWebHooks(max=max)
        filtered_array = [h for h in hook_array if h.targetUrl == url]
        return filtered_array

    def newWebHook(self, name, targetUrl, resource, event, filt):
        """Create Spark webhook."""
        payload = {'name': name, 'targetUrl': targetUrl, 'resource': resource, 'event': event,
                   'filter': filt}
        response = self.restReq.post(self.HOOK_URL, payload=payload)

        if response.status_code != 200:
            response.raise_for_status()
        else:
            r = response.json()
            return Hook(r.get('id'), r.get('name'), r.get('resource'), r.get('event'),
                        r.get('targetUrl'), r.get('filter'))

    def updateWebHook(self, id, name=None, targetUrl=None):
        """
        Update existing webhook.
        Both name and targetUrl are required. For convenience, if one is not provided, it will
        be set to the existing value
        """
        if not name:
            name = self.getById(id).name
        if not targetUrl:
            targetUrl = self.getById(id).targetUrl

        payload = {'name': name, 'targetUrl': targetUrl}
        response = self.restReq.put(self.HOOK_URL, id=id, payload=payload)
        if response.status_code != 200:
            response.raise_for_status()
        else:
            r = response.json()
            return Hook(r.get('id'), r.get('name'), r.get('resource'), r.get('event'),
                        r.get('targetUrl'), r.get('filter'))

    def deleteWebHook(self, hookId):
        """Delete webhook corresponding to the ID provided."""
        response = self.restReq.delete(self.HOOK_URL, id=hookId)
        if response.status_code != 204:
            response.raise_for_status()
        else:
            return True