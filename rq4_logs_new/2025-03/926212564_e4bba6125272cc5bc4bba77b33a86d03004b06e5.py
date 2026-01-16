
class MsSqlDdlParser(object):
    """
    create_ddl ::= 'CREATE' <object_type> ...
    create_table ::= 'CREATE' 'TABLE' <entity_name> '(' <table_item_definition> [',' ...] ')'
    entity_name ::= ['['] <literal> [']']
    table_item_definition ::= <field_definition> | <constraint_definition> | <index_definition>
    field_definition ::= <entity_name> <field_type> [(<size>[,<precision>])] ['NULL' | 'NOT NULL'] ['DEFAULT(' <default> ')'] ['PRIMARY KEY'] [<field_index_definition>]
    size ::= <integer>
    precision ::= <integer>
    default ::= [''''] <literal> ['''']
    field_index_definition ::= 'INDEX' <entity_name> ['CLUSTERED' | 'NONCLUSTERED']
    index_definition ::= <field_index_definition> '(' <entity_name> [',' <entity_name> ...] ')'
    alter_ddl ::= 'ALTER' <object_type> ...
    alter_table ::= 'ALTER' 'TABLE' <entity_name> ['ADD' <entity_name> <field_type> [(<size>[,<precision>])] ['NULL' | 'NOT NULL'] ['DEFAULT(' <default> ')']] ['DROP' 'COLUMN' <entity_name>] ['ALTER' 'COLUMN' <entity_name> <field_type> [(<size>[,<precision>])] ['NULL' | 'NOT NULL'] ['DEFAULT(' <default> ')']]
    """
    def __init__(self):
        ...
