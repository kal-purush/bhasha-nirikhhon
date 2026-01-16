from __future__ import print_function
from __future__ import absolute_import

from comodit_client.api.collection import Collection
from comodit_client.api.entity import Entity


class ApplicationGitCollection(Collection):
    """
    Application git resources collection.
    """

    def _new(self, json_data=None):
        """
        Instantiates a new application git resource object.

        @param json_data: initial state of the object.
        @type json_data: dict
        @return: A new instance of application git resource.
        @rtype: L{Git}
        """

        return Git(self, json_data)


class Git(Entity):
    """
        Application's git resource.
        @see: L{Entity}
    """

    @property
    def branch(self):
        """

        Git's branch name.
        @rtype: string
        """

        return self._get_field("branch")

    @property
    def endpoint(self):
        """

        Git's endpoint name.
        @rtype: string
        """

        return self._get_field("endpoint")

    @property
    def token(self):
        """

        Git's token name.
        @rtype: string
        """

        return self._get_field("token")

    @property
    def authentication_method(self):
        """

        Git's authentication method SSH/HTTPS.
        @rtype: string
        """

        return self._get_field("authenticationMethod")

    @property
    def key_name(self):
        """

        Git's keyName
        @rtype: string
        """

        return self._get_field("keyName")

    def _show(self, indent=0):
        print(" " * indent, self.name + ":")
        print(" " * (indent + 2), "branch:", self.branch)
        print(" " * (indent + 2), "authentication method:", self.authentication_method)
        print(" " * (indent + 2), "endpoint:", self.endpoint)
        print(" " * (indent + 2), "token:", self.token)
        print(" " * (indent + 2), "keyName:", self.key_name)