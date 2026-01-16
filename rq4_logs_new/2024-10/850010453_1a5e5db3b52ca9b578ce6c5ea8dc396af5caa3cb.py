import builtins
from   collections import defaultdict
import functools
import typing
import unittest

from . import *

class _TestMethod(typing.Protocol):

    def __call__(self:'Tests', unit:model.Unit): ...

class Tests(unittest.TestCase): 

    def setUp(self):

        self.tr = TestRegistrator()
        self.th = self.tr.handler(self)

    def _file(file_name:str):

        def a(f:_TestMethod):

            def test_(self:'Tests'):
    
                builtins.print(f'\nTest file: {file_name}',end=' ')
                unit:model.Unit|None = None
                with open(testfile_path(file_name), mode='r') as file:

                    unit = load(file.read())

                f(self, unit)

            return test_
        
        return a

    def getAsserted[K,V](self, d:dict[K,V], k:K):

        self.assertIn(k, d)
        return d[k]

    @_file('Test1.java')
    def test_1(self, unit:model.Unit):

        self.assertEqual(unit.package, 'project.tests.java_files')
        self.assertIn   ('java.util.Map', unit.imports)
        self.assertEqual(len(unit.imports), 1)
        self.assertIn   ('Test1'        , unit.classes)
        clas = unit.classes['Test1']
        self.assertIs   (clas.header.access             , model.AccessModifiers.PUBLIC)
        self.assertEqual(len(clas.header.annotations)   , 0)
        self.assertIs   (clas.header.finality           , model.FinalityTypes.DEFAULT)
        self.assertIs   (clas.header.generics           , None)
        self.assertEqual(len(clas.header.inherit[model.InheritanceTypes.IMPLEMENTS]), 0)
        self.assertEqual(len(clas.header.inherit[model.InheritanceTypes.EXTENDS   ]), 0)
        self.assertIs   (clas.header.signature          , None)
        self.assertIs   (clas.header.type               , model.ClassTypes.CLASS)
        attr_a1 = self.getAsserted(clas.members.attributes       , 'a1')
        attr_a2 = self.getAsserted(clas.members.static_attributes, 'a2')
        attr_a3 = self.getAsserted(clas.members.attributes       , 'a3')
        attr_a4 = self.getAsserted(clas.members.static_attributes, 'a4')
        attr_c5 = self.getAsserted(clas.members.static_attributes, 'c5')
        self.assertEqual(attr_a1, model.Attribute(type=model.Type('int'), access=model.AccessModifiers.PRIVATE))
        self.assertIs   (attr_a1.access   , model.AccessModifiers.PRIVATE)
        self.assertIs   (attr_a1.final    , False)
        self.assertIs   (attr_a1.transient, False)
        self.assertEqual(attr_a1.type     , model.Type(name='int'))
        self.assertIs   (attr_a1.value    , None)
        self.assertIs   (attr_a1.volatile , False)
        self.assertEqual(attr_a2, model.Attribute(type=model.Type('boolean')))
        self.assertEqual(attr_a3, model.Attribute(type=model.Type('String', array_dim=1) , access=model.AccessModifiers.PROTECTED))
        self.assertEqual(attr_a4, model.Attribute(type=model.Type('Object')              , access=model.AccessModifiers.PUBLIC))
        self.assertEqual(attr_c5, model.Attribute(type=model.Type('Object', array_dim=4) , access=model.AccessModifiers.PUBLIC, value='null', final =True))
        self.assertEqual(clas.members.initializer.body       , '\n'+8*' '+'System.out.println("Hello");\n'        +4*' ')
        self.assertEqual(clas.members.static_initializer.body, '\n'+8*' '+'System.out.println("Hello, static");\n'+4*' ')
        self.assertEqual(len(clas.members.constructors), 3)
        constructors_by_access = functools.reduce(lambda l, constructor: (l[constructor.access].append(constructor), l,)[-1], clas.members.constructors, defaultdict(list))
        self.assertEqual(len(constructors_by_access[model.AccessModifiers.PUBLIC   ]), 1)
        self.assertEqual(constructors_by_access[model.AccessModifiers.PUBLIC][0], model.Constructor(access=model.AccessModifiers.PUBLIC,  args={'properties':model.Argument(type=model.Type('Map'    , generics=[model.Type(name='String'),model.Type(name='String')])),
                                                                                                                                                'awesome'   :model.Argument(type=model.Type('Boolean'))}, body=''))
        self.assertEqual(len(constructors_by_access[model.AccessModifiers.PRIVATE  ]), 1)
        self.assertEqual(len(constructors_by_access[model.AccessModifiers.PROTECTED]), 0)
        self.assertEqual(len(constructors_by_access[model.AccessModifiers.DEFAULT  ]), 1)

    @_file('Test2.java')
    def test_2(self, unit:model.Unit):

        self.tr.r_package       (entity.PackageDeclaration    (name='project.tests.java_files'))
        self.tr.r_import        (entity.ImportDeclaration     (name='java.util.*'))
        self.tr.r_class         (entity.ClassHeaderDeclaration(name='Test2'                     , header=model.ClassHeader(access=model.AccessModifiers.PUBLIC)))
        self.tr.r_method        (entity.MethodDeclaration     (name='Test2', method=model.Method(access=model.AccessModifiers.PUBLIC, type=model.Type(name='void'), args={'ints':model.Argument(annotations=[model.Annotation(name='QueryParam')], type=model.Type(name='List', generics=[model.Type(name='Integer', annotations=[model.Annotation(name='NonNull')])]))}, body='')))
        self.tr.r_constructor   (entity.ConstructorDeclaration(model.Constructor(access=model.AccessModifiers.PUBLIC,                                               args={'ints':model.Argument(annotations=[model.Annotation(name='QueryParam')], type=model.Type(name='List', generics=[model.Type(name='Integer', annotations=[model.Annotation(name='NonNull')])]))}, body='')))
        self.tr.r_method        (entity.MethodDeclaration     (name='Test2', method=model.Method(access=model.AccessModifiers.PUBLIC, type=model.Type(name='void'), args={'ints':model.Argument(annotations=[model.Annotation(name='QueryParam'), model.Annotation(name='Foo', args=['"Bar"', '"Baz"'])], type=model.Type(name='List', generics=[model.Type(name='Integer', annotations=[model.Annotation(name='NonNull')])]))}, body='')))
        self.tr.r_class_end     ()