"""`SConsArguments.Arguments`

TODO: Write documentation
"""

#
# Copyright (c) 2015 by Pawel Tomulik
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE

__docformat__ = "restructuredText"

from .Util import ENV, VAR, OPT, ALL, UNDEFINED
from .Util import _compose_mappings, _invert_dict, _build_resubst_dict
from .VariablesWrapper import _VariablesWrapper
from .Proxy import _ArgumentsProxy

#############################################################################
class _Arguments(object):
    #========================================================================
    """Binds *arguments* to their *endpoints* (construction variables,
    command-line variables and command-line options).

    In fact, the only internal data the object holds is a list of supplementary
    dictionaries to map the names of variables between the namespace of
    *arguments* and namespaces of their *endpoints*.

    **Note**:

        In several places we use ``ns`` as placeholder for one of the `ENV`,
        `VAR` or `OPT` *endpoint* selectors.
    """
    #========================================================================

    #========================================================================
    def __init__(self, decls):
        # -------------------------------------------------------------------
        """Initializes `_Arguments` object from `_ArgumentDeclarations`.

        :Parameters:
            decls : `_ArgumentDeclarations`
                declarations of *arguments*,
        """
        # -------------------------------------------------------------------
        self.__keys = decls.keys()
        self.__init_supp_dicts(decls)

    #========================================================================
    def __reset_supp_dicts(self):
        """Initialize empty supplementary dictionaries to empty state. This is
        internal method and IS **NOT a part of public API**"""
        self._rename_dict = [{} for n in range(0,ALL)]
        self._irename_dict = [{} for n in range(0,ALL)]
        self._resubst_dict = [{} for n in range(0,ALL)]
        self._iresubst_dict = [{} for n in range(0,ALL)]

    #========================================================================
    def __init_supp_dicts(self, decls):
        """Initialize supplementary dictionaries according to variable
        declarations. This is internal method and IS **NOT a part of public
        API**"""
        self.__reset_supp_dicts()
        if decls is not None:
            for ns in range(0,ALL):
                self._rename_dict[ns] = decls.get_rename_dict(ns)
                self._irename_dict[ns] = decls.get_irename_dict(ns)
                self._resubst_dict[ns] = decls.get_resubst_dict(ns)
                self._iresubst_dict[ns] = decls.get_iresubst_dict(ns)

    #========================================================================
    def VarEnvProxy(self, env, *args, **kw):
        """Return "VAR-to-ENV" proxy. With this proxy you may access
        construction variables in SCons environment `env` while using keys from
        `VAR` namespace (command-line variables)."""
        rename = _compose_mappings(self._irename_dict[VAR], self._rename_dict[ENV])
        irename = _invert_dict(rename)
        resubst = _build_resubst_dict(rename)
        iresubst = _build_resubst_dict(irename)
        return _ArgumentsProxy(env, rename, resubst, irename, iresubst, *args, **kw)

    #========================================================================
    def OptEnvProxy(self, env, *args, **kw):
        """Return "OPT-to-ENV" proxy. With this proxy you may access
        construction variables in SCons environment `env` while using keys from
        `OPT` namespace (command-line options)."""
        rename = _compose_mappings(self._irename_dict[OPT], self._rename_dict[ENV])
        irename = _invert_dict(rename)
        resubst = _build_resubst_dict(rename)
        iresubst = _build_resubst_dict(irename)
        return _ArgumentsProxy(env, rename, resubst, irename, iresubst, *args, **kw)

    #========================================================================
    def EnvProxy(self, env, *args, **kw):
        """Return proxy to SCons environment `env` which uses *argument* names
        to access corresponding construction variables in SCons environment
        `env`."""
        return _ArgumentsProxy(env, self._rename_dict[ENV], self._resubst_dict[ENV],
                                  self._irename_dict[ENV], self._iresubst_dict[ENV],
                                  *args, **kw)

    #========================================================================
    def get_keys(self):
        """Return the list of *argument* names."""
        return self.__keys[:]

    #========================================================================
    def get_key(self, ns, key):
        #--------------------------------------------------------------------
        """Map *argument* to a variable from `ENV`, `VAR` or `OPT` namespace.
        Maps only names (keys), not values.

        :Parameters:
            ns : int
                namespace identifier, one of `ENV`, `VAR` or `OPT`,
            key : string
                the input key (*argument* name) to be mapped.

        :Return:
            The name of mapped variable (resultant key) from namespace ``ns``.

        **Example**::

            import SConsArguments
            decls = SConsArguments.DeclareArguments(
                foo = { 'env_key' : 'ENV_FOO',
                        'var_key' : 'VAR_FOO',
                        'opt_key' : 'OPT_FOO', 'option' : '--foo' },
                bar = { 'env_key' : 'ENV_BAR',
                        'var_key' : 'VAR_BAR',
                        'opt_key' : 'OPT_BAR', 'option' : '--bar' }
            )
            args = decls.Commit()
            assert(args.get_key(SConsArguments.ENV, 'foo') == 'ENV_FOO')
            assert(args.get_key(SConsArguments.VAR, 'foo') == 'VAR_FOO')
            assert(args.get_key(SConsArguments.OPT, 'foo') == 'OPT_FOO')
            assert(args.get_key(SConsArguments.ENV, 'bar') == 'ENV_BAR')
            assert(args.get_key(SConsArguments.VAR, 'bar') == 'VAR_BAR')
            assert(args.get_key(SConsArguments.OPT, 'bar') == 'OPT_BAR')
        """
        #--------------------------------------------------------------------
        return self._rename_dict[ns][key]

    #========================================================================
    def get_env_key(self, key):
        """Similar to `get_key(ENV,key)`"""
        return self._rename_dict[ENV][key]

    #========================================================================
    def get_var_key(self, key):
        """Similar to `get_key(VAR,key)`"""
        return self._rename_dict[VAR][key]

    #========================================================================
    def get_opt_key(self, key):
        """Similar to `get_key(OPT,key)`"""
        return self._rename_dict[OPT][key]

    #========================================================================
    def update_env_from_vars(self, env, variables, args=None):
        #--------------------------------------------------------------------
        """Update construction variables in SCons environment
        (``env["VARIABLE"]=VALUE``) according to values stored in their
        corresponding command-line variables (``variable=value``).

        **Note**:

            This function calls the `variables.Update(proxy[,args])`_ method
            passing `env` proxy (see `_ArgumentsProxy`) to the method in
            order to enable mappings between ``ENV`` and ``VAR`` namespaces.

        :Parameters:
            env
                `SCons environment`_ object to be updated,
            variables
                `SCons variables`_ object to take values from

        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        .. _SCons variables: http://www.scons.org/doc/HTML/scons-user.html#sect-command-line-variables
        .. _variables.Update(proxy[,args]): http://www.scons.org/doc/latest/HTML/scons-api/SCons.Variables.Variables-class.html#Update
        """
        #--------------------------------------------------------------------
        proxy = self.VarEnvProxy(env)
        _VariablesWrapper(variables).Update(proxy,args)


    #========================================================================
    def update_env_from_opts(self, env):
        #--------------------------------------------------------------------
        """Update construction variables in SCons environment
        (``env["VARIABLE"]=VALUE``) according to values stored in their
        corresponding `command-line options`_ (``--option=value``).

        :Parameters:
            env
                `SCons environment`_ object to be updated

        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        .. _command-line options: http://www.scons.org/doc/HTML/scons-user.html#sect-command-line-options
        """
        #--------------------------------------------------------------------
        from SCons.Script.Main import GetOption
        proxy = self.OptEnvProxy(env)
        for opt_key in self._irename_dict[OPT]:
            opt_value = GetOption(opt_key)
            # FIXME: why not pass None to environment (currently it's skipped)?
            if opt_value is not None and opt_value is not UNDEFINED:
                proxy[opt_key] = opt_value

    #========================================================================
    def UpdateEnvironment(self, env, variables=None, use_options=False, args=None):
        #--------------------------------------------------------------------
        """Update construction variables in SCons environment
        (``env["VARIABLE"]=VALUE``) according to values stored in their
        corresponding `command-line variables`_ (``variable=value``) and/or
        `command-line options`_ (``--option=value``).

        :Parameters:
            env
                `SCons environment`_ object to update,
            variables : ``SCons.Variables.Variables`` | None
                if not ``None``, it should be a `SCons.Variables.Variables`_
                object with `SCons variables`_ to retrieve values from,
            use_options : boolean
                if ``True``, `command-line options`_ are taken into account
                when updating `env`.
            args
                if not ``None``, passed verbatim to `update_env_from_vars()`.

        .. _SCons.Variables.Variables: http://www.scons.org/doc/latest/HTML/scons-api/SCons.Variables.Variables-class.html
        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        .. _command-line variables: http://www.scons.org/doc/HTML/scons-user.html#sect-command-line-variables
        .. _SCons variables: http://www.scons.org/doc/HTML/scons-user.html#sect-command-line-variables
        .. _command-line options: http://www.scons.org/doc/HTML/scons-user.html#sect-command-line-options
        """
        #--------------------------------------------------------------------
        # TODO: implement priority?
        if variables is not None:
            self.update_env_from_vars(env, variables, args)
        if use_options:
            self.update_env_from_opts(env)

    def SaveVariables(self, variables, filename, env):
        #--------------------------------------------------------------------
        """Save the `variables` to file, while mapping appropriately their names.

        :Parameters:
            variables : ``SCons.Variables.Variables``
                if not ``None``, it should be an instance of
                `SCons.Variables.Variables`_; this object is used to save
                SCons variables,
            filename : string
                name of the file to save into
            env
                `SCons environment`_ object to update,

        .. _SCons.Variables.Variables: http://www.scons.org/doc/latest/HTML/scons-api/SCons.Variables.Variables-class.html
        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        """
        #--------------------------------------------------------------------
        proxy = self.VarEnvProxy(env)
        _VariablesWrapper(variables).Save(filename, proxy)

    def GenerateVariablesHelpText(self, variables, env, *args, **kw):
        #--------------------------------------------------------------------
        """Generate help text for `variables` using
        ``variables.GenerateHelpText()``.

        Note:
            this function handles properly mapping names between namespace
            of SCons command line variables and namespace of SCons construction
            variables.

        :Parameters:
            variables : ``SCons.Variables.Variables``
                if not ``None``, it should be an instance of
                `SCons.Variables.Variables`_; this object is used to save
                SCons variables,
            env
                `SCons environment`_ object to update,
            args
                other arguments passed verbatim to ``GenerateHelpText()``

        .. _SCons.Variables.Variables: http://www.scons.org/doc/latest/HTML/scons-api/SCons.Variables.Variables-class.html
        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        """
        #--------------------------------------------------------------------
        proxy = self.VarEnvProxy(env)
        return _VariablesWrapper(variables).GenerateHelpText(proxy, *args, **kw)

    def GetCurrentValues(self, env):
        #--------------------------------------------------------------------
        """Get current values of *arguments* stored in environment

        :Parameters:
            env
                `SCons environment`_ object to update,
        :Return:
            Dict containing current values.

        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        """
        #--------------------------------------------------------------------
        res = {}
        proxy1 = self.EnvProxy(env, strict = True)
        proxy2 = self.EnvProxy(res, strict = True)
        for k in self.__keys:
            try:
                v = proxy1[k]
            except KeyError:
                # note: KeyError can be triggered by env.
                pass
            else:
                proxy2[k] = v
        return res

    @staticmethod
    def _is_unaltered(cur, org, v):
        #--------------------------------------------------------------------
        """Whether variable ``cur[v]`` has changed its value w.r.t. ``org[v]``.

        :Parameters:
            cur
                *argument* proxy to `SCons Environment`_ or simply to a
                dictionary which holds current values of variables. An
                `_ArgumentsProxy` object.
            org
                *argument* proxy to a dictionary containig original values of
                *arguments*.
            v
                *argument* name to be verified.

        :Return:
            ``True`` or ``False``

        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        """
        #--------------------------------------------------------------------
        result = False
        try:
            curval = cur[v]
        except KeyError:
            try:
                org[v]
            except KeyError:
                result = True
        else:
            try:
                orgval = org[v]
            except KeyError:
                # cur[v] has been created in the meantime
                pass
            else:
                if curval == orgval:
                    result = True
        return result

    def GetAltered(self, env, org):
        #--------------------------------------------------------------------
        """Return *arguments* stored in `env` which have changed with respect to `org`.

        :Parameters:
            env
                `SCons environment`_ object or simply a dictionary which holds
                current values of *arguments*.
            org
                Dict containing original (default) values of variables.

        :Return:
            Dictionary with *arguments* that changed their value. The keys are
            in ENV namespace (i.e. they're same as keys in `env`).

        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        """
        #--------------------------------------------------------------------
        res = {}
        envp = self.EnvProxy(env, strict = True)
        orgp = self.EnvProxy(org, strict = True)
        resp = self.EnvProxy(res, strict = True)
        for k in self.__keys:
            if not _Arguments._is_unaltered(envp, orgp, k):
                resp[k] = envp[k]
        return res

    def OverwriteUnaltered(self, env, org, new):
        #--------------------------------------------------------------------
        """Overwrite unaltered *arguments* in `env` with corresponding values
        from `new`.

        For every *argument* stored in `env`, if its value is same as
        corresponding value in `org` the value in `env` gets replaced with
        corresponding value in `new`.

        :Parameters:
            env
                `SCons environment`_ object or simply a dict which holds
                curreng values of *arguments*. It's also being updated with new
                values; the keys in `env` should be in ENV namespace,
            org
                Dict containing original (default) values of variables;
                the keys in `org` should be in ENV namespace,
            new
                Dict with new values to be used to update entries in `env`;
                the keys in `new` should identify *argument* names (they're
                not in ENV/VAR/OPT namespace as opposite to `env` and `org`).

        :Return:
            A dictionary containing only the values from `new` that were
            assigned to `env`.

        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        """
        #--------------------------------------------------------------------
        chg = {}
        envp = self.EnvProxy(env, strict = True)
        orgp = self.EnvProxy(org, strict = True)
        chgp = self.EnvProxy(chg, strict = True)
        for k in self.__keys:
            if _Arguments._is_unaltered(envp, orgp, k):
                try:
                    envp[k] = new[k]
                    chgp[k] = new[k] # Backup the values we've changed.
                except KeyError:
                    pass
        return chg

    def ReplaceUnaltered(self, env, org, new):
        #--------------------------------------------------------------------
        """Return result of replacing *arguments* stored in `env` with
        corresponding values from `new`, while replacing only unaltered
        values (see `_is_unaltered()`).

        :Parameters:
            env
                `SCons environment`_ object or simply a dict which holds
                current values of *arguments*; the keys in `env` should be in
                ENV namespace,
            org
                Dict containing orginal (default) values of variables;
                the keys in `org` should be in ENV namespace,
            new
                Dict with new values to be used instead of those from `env`;
                the keys in `new` should identify *argument* names (they're
                not in ENV/VAR/OPT namespace as opposite to `env` and `org`).

        :Return:
            New dictionary with values taken from `env` with some of them
            overwriten by corresponding values from `new`; the returned dict
            will only contain variables that are present in in this object;

        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        """
        #--------------------------------------------------------------------
        res = {}
        envp = self.EnvProxy(env, strict = True)
        orgp = self.EnvProxy(org, strict = True)
        resp = self.EnvProxy(res, strict = True)
        for k in self.__keys:
            if _Arguments._is_unaltered(envp, orgp, k):
                try:
                    resp[k] = new[k]
                except KeyError:
                    try:
                        resp[k] = envp[k]
                    except KeyError:
                        # envp[k] may not exist (this handles _undefs)
                        pass
            else:
                try:
                    resp[k] = envp[k]
                except KeyError:
                    # envp[k] may not exist (this handles _undefs)
                    pass
        return res

    def Postprocess(self, env, variables=None, use_options=False, ose={},
                    args=None, filename=None):
        #--------------------------------------------------------------------
        """Postprocess `variables` and **options** updating variables in
        `env` and optionally saving them to file.

        This method gathers values from `variables`, command-line options and
        writes them to `env`. After that it optionally saves the variables
        to file (if `filename` is given) and updates variables with values
        from `ose` (only those that were not changed by `variables` nor the
        command-line options).

        The effect of this method is the following:

        - command line `variables` are handled,
        - command line options are handled,
        - command line `variables` are saved to filename if requested,
        - additional source `ose` (usually ``os.environ``) handled,

        The values from `ose` are not written to file. Also, they influence
        only variables that were not set by `variables` nor the command-line
        options. The intent is to not let the variables from OS environment to
        overwrite these provided by commandline (or retrieved from file).

        **Example**::

            # SConstruct
            import os
            from SConsArguments import DeclareArguments

            env = Environment()
            var = Variables('.scons.variables')
            decls = DeclareArguments( foo = { 'env_key' : 'foo', 'var_key' : 'foo' } )
            args = decls.Commit(env, var, False)
            args.Postprocess(env, var, False, os.environ, filename = '.scons.variables')

            print "env['foo']: %r" % env['foo']

        Sample session (the sequence order of the following commands is
        important)::

            ptomulik@tea:$ rm -f .scons.variables

            ptomulik@tea:$ scons -Q
            env['foo']: None
            scons: `.' is up to date.

            ptomulik@tea:$ foo=ose scons -Q
            env['foo']: 'ose'
            scons: `.' is up to date.

            ptomulik@tea:$ scons -Q
            env['foo']: None
            scons: `.' is up to date.

            ptomulik@tea:$ foo=ose scons -Q foo=var
            env['foo']: 'var'
            scons: `.' is up to date.

            ptomulik@tea:$ cat .scons.variables
            foo = 'var'

            ptomulik@tea:$ scons -Q
            env['foo']: 'var'
            scons: `.' is up to date.

            ptomulik@tea:$ foo=ose scons -Q
            env['foo']: 'var'
            scons: `.' is up to date.

            ptomulik@tea:$ rm -f .scons.variables
            ptomulik@tea:$ foo=ose scons -Q
            env['foo']: 'ose'
            scons: `.' is up to date.

        :Parameters:
            env
                `SCons environment`_ object which holds current values of
                *arguments*.
            variables : ``SCons.Variables.Variables`` | None
                if not ``None``, it should be a `SCons.Variables.Variables`_
                object with `SCons variables`_ to retrieve values from,
            use_options : boolean
                if ``True``, `command-line options`_ are taken into account
                when updating `env`.
            ose : dict
                third source of data, usually taken from ``os.environ``
            args
                passed as `args` to `UpdateEnvironment()`.
            filename : str|None
                Name of the file to save current values of `variables`.
                By default (``None``) variables are not saved.

        :Return:
            New dictionary with only entries updated by either of the data
            sources (variables, options or ose).

        :Note:
            Often you will have to preprocess ``os.environ`` before passing it
            as `ose`. This is necessary especially when your *argument* use
            ``converter``. In that case you have to pass values from
            ``os.environ`` through a similar converter too.

        .. _SCons.Variables.Variables: http://www.scons.org/doc/latest/HTML/scons-api/SCons.Variables.Variables-class.html
        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        .. _SCons variables: http://www.scons.org/doc/HTML/scons-user.html#sect-command-line-variables
        .. _command-line options: http://www.scons.org/doc/HTML/scons-user.html#sect-command-line-options
        """
        #--------------------------------------------------------------------
        org = self.GetCurrentValues(env)
        self.UpdateEnvironment(env, variables, use_options, args)
        alt = self.GetAltered(env, org)
        if filename:
            self.SaveVariables(variables, filename, env)
        chg = self.OverwriteUnaltered(env, org, ose)
        alt.update(chg)
        return alt

    def Demangle(self, env):
        #--------------------------------------------------------------------
        """Transform-back variables from ``ENV`` namespace to namespace of
        *arguments*.

        It is possible, that names of *arguments* are not same as names of
        their associated construction variables in `env`. This method
        returns a new dictionary having original *argument* names as keys
        and with placeholders transformed-back to the namespace of *arguments*.

        **Example**

        .. python::


            # SConstruct
            from SConsArguments import DeclareArguments, UNDEFINED

            env = Environment()
            decls = DeclareArguments( foo = { 'env_key' : 'env_foo', 'default' : 'default ${foo}' } )
            args = decls.Commit(env)
            vars = args.Demangle(env)

            print "env['foo']: %r" % env.get('foo', UNDEFINED)
            print "env['env_foo']: %r" % env.get('env_foo', UNDEFINED)
            print "vars['foo']: %r" % vars.get('foo', UNDEFINED)

        The result of running the above SCons script is::

            ptomulik@tea:$ scons -Q
            env['foo']: UNDEFINED
            env['env_foo']: 'default ${env_foo}'
            vars['foo']: 'default ${foo}'
            scons: `.' is up to date.

        :Parameters:
            env
                `SCons environment`_ object or simply a dict which holds
                current values of *arguments*.

        :Return:
            New dictionary with keys (and placeholders appearing in values)
            transformed back to namespace of *arguments*.

        .. _SCons environment:  http://www.scons.org/doc/HTML/scons-user.html#chap-environments
        """
        #--------------------------------------------------------------------
        proxy = self.EnvProxy(env, strict=True)
        res = {}
        for k in self.__keys:
            try:
                res[k] = proxy[k]
            except KeyError:
                pass
        return res

# Local Variables:
# # tab-width:4
# # indent-tabs-mode:nil
# # End:
# vim: set syntax=python expandtab tabstop=4 shiftwidth=4: