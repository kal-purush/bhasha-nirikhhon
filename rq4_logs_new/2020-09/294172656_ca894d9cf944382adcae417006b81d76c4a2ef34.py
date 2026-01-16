# config:utf8

class SGSchema(object):
    api = None
    sgClasses = None
    DEFAULT_RETURN = dict()
    __schema_entities = None

    @classmethod
    def set_api(cls, api):
        if cls.api:
            return
        cls.api = api
        cls.DEFAULT_RETURN = {key: list() for key in cls._entities()}
        cls.DEFAULT_RETURN.update({
            '_': [],
            'Asset': ['id', 'code', 'sg_asset_type', 'sg_status_list'],
            'Note': ['id', 'addressings_to', 'user', 'tasks','note_links'],
            'Project': ['id', 'name', 'sg_pm', 'sg_type'],
            'Reply': ['id', 'content', 'entity', 'user'],
            'Shot': ['id', 'code', 'sg_shot_type', 'sg_status_list'],
            'Task': [
                'id', 'content', 'entity',
                'task_assignees', 'task_reviewers'],
            'HumanUser': ['id', 'name', 'email'],
            'Version': ['id', 'project', 'user', 'entity', 'code', 'sg_task']
        })
        cls.sgClasses = SG_Classes()

    @classmethod
    def _entities(cls):
        if cls.api is None:
            raise RuntimeError('use set_api first')
        if not cls.__schema_entities:
            cls.__schema_entities = cls.api.schema_entity_read()
        return cls.__schema_entities


class SG_Classes(object):

    def __getattr__(self, attr_):
        if attr_ not in SGSchema.DEFAULT_RETURN:
            raise AttributeError("Shotgun has no such entity:%s" % (attr_))
        if attr_ in self.__dict__:
            return self.__dict__[attr_]
        else:
            cls = type(attr_, (SG_Base,), {})
            cls.type_ = attr_
            self.__dict__[attr_] = cls
            return self.__dict__[attr_]


class SG_Base(object):
    type_ = '_'

    def __init__(self, id_, api, logger):
        self.id_ = id_
        self.api = api
        self.logger = logger
        self.return_list = SGSchema.DEFAULT_RETURN[self.type_]
        self._attrs = dict()
        self._sg_attrs = dict()
        self._schema_dict = None
        if self.type_ != '_':
            self._find_attrs()

    def _find_attrs(self):
        sg_data = self.api.find_one(
            self.type_,
            [['id', 'is', self.id_]],
            self.return_list)
        self._sg_attrs.update(sg_data)

    def _schema(self):
        if not self._schema_dict:
            self._schema_dict = self.api.schema_field_read(self.type_)
        return self._schema_dict

    def __getattr__(self, attr_):
        if attr_ in self._schema():
            if attr_ not in self.return_list:
                self.return_list.append(attr_)
            self._find_attrs()
            return self._attr_get_(attr_)
        else:
            raise AttributeError(
                'shotgun entity type:%s, has no attribute:%s\nvalid value are %s' % (self.type_, attr_, self._schema_dict.keys()))

    def _attr_get_(self, attr_):
        if self._attrs.get(attr_, None):
            return self._attrs[attr_]

        enty_val = self._sg_attrs[attr_]
        if not enty_val:
            return enty_val
        if type(enty_val) is list:
            self._attrs[attr_] = [self._sg2obj(enty['type'], enty['id']) for enty in enty_val]
        elif type(enty_val) is dict:
            self._attrs[attr_] = self._sg2obj(enty_val['type'], enty_val['id'])
        else:
            self._attrs[attr_] = enty_val
        return self._attrs[attr_]

    def _sg2obj(self, type_, id_):
        sg_enty_type = 'SG_' + type_
        cls = type(sg_enty_type, (SG_Base,), {})
        cls.type_ = type_
        enty = cls(id_, self.api, self.logger)
        return enty

    def name_(self):
        name_code = ''
        if self.type_ in ('Asset', 'Shot', 'Version'):
            name_code = 'code'
        elif self.type_ in ('Task',):
            name_code = 'content'
        elif self.type_ in ('Note'):
            name_code = 'cached_display_name'
        else:
            name_code = 'name'
        return self.__getattr__(name_code)

    def __str__(self):
        return '<Shotgun:%s(%s), name:%s>' % (self.type_, self.id_, self.name_())

    def __eq__(self, obj):
        if not isinstance(obj, SG_Base):
            raise ValueError('except %s, get "%s"' % (type(self), type(obj)))
        if self.type_ != obj.type_: raise ValueError('except %s, get "%s"' % (self.type_, obj.type_))
        if self.id_ == obj.id_:
            return True
        else:
            return False