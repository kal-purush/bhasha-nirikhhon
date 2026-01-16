#!/usr/bin/python

# dbus_parse_test.py
#  Testing for D-Bus dissection
#  Copyright 2015, Lucas Hong Tran <hongtd2k@gmail.com>
# 
#  Protocol specification available at http://dbus.freedesktop.org/doc/dbus-specification.html
# 
# 
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License
#  as published by the Free Software Foundation; either version 2
#  of the License, or (at your option) any later version.
# 
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
# 
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.


import os
import sys
from dbus import *
import dbus.service
import time
import pprint
import random
import sys

DBUS_TEST_SUITE = [

#fix type test case
    [ "string", 
        lambda :  (dbus.String(s[0]), dbus.String(s[1])), 
        lambda :"STRING:%s,STRING:%s" % (s[0], s[1]) 
    ],
    [ "(boolbool)", 
        lambda :  dbus.Struct( (dbus.Boolean(bool[0]), dbus.Boolean(bool[1])) ),
        lambda :"STRUCT(BOOLEAN:%d,BOOLEAN:%d)" % (bool[0], bool[1]) 
     ],
    [ "(bytebyte)", 
        #lambda :  dbus.Struct(dbus.Byte(b[0]), dbus.Byte(b[1])), 
        lambda :  dbus.Struct( (dbus.Byte(b[0]), dbus.Byte(b[1])) ),
        lambda :"STRUCT(BYTE:%d,BYTE:%d)" % (b[0], b[1]) 
     ],
    [ "(uint16int16)", 
        lambda :  dbus.Struct((dbus.UInt16(uint16[0]), dbus.Int16(int16[1]))), 
        lambda :"STRUCT(UINT16:%u,INT16:%d)" % (uint16[0], int16[1]) 
     ],
    [ "(uint32int32)", 
        lambda :  dbus.Struct((dbus.UInt32(uint32[0]), dbus.Int32(int32[1]))), 
        lambda :"STRUCT(UINT32:%u,INT32:%d)" % (uint32[0], int32[1]) 
     ],
    [ "(uint64int64)", 
        lambda :  dbus.Struct((dbus.UInt64(uint64[0]), dbus.Int64(int64[1]))), 
        lambda :"STRUCT(UINT32:%lu,INT32:%ld)" % (uint64[0], int64[1]) 
     ],  
     [ "(floatfloat)", 
        lambda :  dbus.Struct((dbus.Double(dbl[0]), dbus.Double(dbl[1]))), 
        lambda :"STRUCT(DOUBLE:%f,DOUBLE:%f)" % (dbl[0], dbl[1]) 
     ],  

#container test cases
#variant testing
                   
     [ "(variantuint16)", 
        lambda :  dbus.UInt16( uint16[0]
                               , variant_level = 1), 
        lambda :"VARIANT(UINT16({0})))" .format (uint16[0]) 
     ],     
     [ "(variantint16)", 
        lambda :  dbus.Int16( int16[0]
                               , variant_level = 1), 
        lambda :"VARIANT(INT16({0})))" .format (int16[0]) 
     ],  
     [ "(variantuint32)", 
        lambda :  dbus.UInt32( uint32[0]
                               , variant_level = 1), 
        lambda :"VARIANT(UINT32({0})))" .format (uint32[0]) 
     ],     
     [ "(variantint32)", 
        lambda :  dbus.Int32( int32[0]
                               , variant_level = 1), 
        lambda :"VARIANT(INT32({0})))" .format (int32[0]) 
     ], 
     [ "(variantuint64)", 
        lambda :  dbus.UInt64( uint64[0]
                               , variant_level = 1), 
        lambda :"VARIANT(UINT64({0})))" .format (uint64[0]) 
     ],                     
     [ "(variantint64)", 
        lambda :  dbus.Int64( int64[0]
                               , variant_level = 1), 
        lambda :"VARIANT(INT64({0})))" .format (int64[0]) 
     ], 
     [ "(variantdouble)", 
        lambda :  dbus.Double( dbl[0]
                               , variant_level = 1), 
        lambda :"VARIANT(DOUBLE({0})))" .format (dbl[0]) 
     ], 
     [ "(variantbyte)", 
        lambda :  dbus.Byte( b[0] , variant_level = 1), 
        lambda : "VARIANT(BYTE(%d))" % (b[0]) 
     ], 
     [ "(variantboolean)", 
        lambda :  dbus.Boolean( bool[0]
                               , variant_level = 1), 
        lambda :"VARIANT(BOOLEAN({0})))" .format (bool[0]) 
     ], 
     [ "(variantobjpath)", 
        lambda :  dbus.ObjectPath( "/" + s[0]
                               , variant_level = 1), 
        lambda :"VARIANT(OBJECT_PATH(/{0})))" .format (s[0]) 
     ], 
                   
     [ "(varianarraystring)", 
        lambda :  dbus.Array( 
                             ( [dbus.String(s[0]), 
                                dbus.String(s[1]) ]
                              ), variant_level = 1), 
        lambda :"VARIANT(ARRAY(STRING:{0},STRING:{1}))" .format (s[0], s[1]) 
     ], 
     [ "(variantvariantvariantvariantvariantarraystring)", 
        lambda :  dbus.Array( 
                             ( [dbus.String(s[0]), 
                                dbus.String(s[1]) ]
                              ), variant_level = 5), 
        lambda :"VARIANT(VARIANT(VARIANT(VARIANT(VARIANT(ARRAY(STRING:{0},STRING:{1}))))))" .format (s[0], s[1]) 
     ],                     
     [ "(variantarrayuint16)", 
        lambda :  dbus.Array(
                             [ dbus.UInt16( uint16[0]), dbus.UInt16( uint16[1]) ]
                               , variant_level = 1), 
        lambda :"VARIANT(ARRAY(UINT16({0}),UINT16({1}))))" .format (uint16[0], uint16[1] ) 
     ],     
     [ "(variantarrayint16)", 
        lambda :  dbus.Array(
                             [dbus.Int16( int16[0]), dbus.Int16( int16[1])]
                               , variant_level = 1), 
        lambda :"VARIANT(INT16({0}),INT16({1})))" .format (int16[0], int16[1]) 
     ],  
     [ "(variantarrayuint32)", 
        lambda :  dbus.Array(
                             [dbus.UInt32( uint32[0]), dbus.UInt32( uint32[1]) ]
                               , variant_level = 1), 
        lambda :"VARIANT(UINT32({0}),UINT32({1})))" .format (uint32[0], uint32[1]) 
     ],     
     [ "(variantarrayint32)", 
        lambda :  dbus.Array(
                             [dbus.Int32( int32[0]), dbus.Int32( int32[1])]
                               , variant_level = 1), 
        lambda :"VARIANT(INT32({0}),INT32({1})))" .format (int32[0], int32[1]) 
     ], 
     [ "(variantarrayuint64)", 
        lambda :  dbus.Array(
                             [dbus.UInt64( uint64[0]), dbus.UInt64( uint64[1])]
                               , variant_level = 1), 
        lambda :"VARIANT(UINT64({0}),UINT64({1})))" .format (uint64[0], uint64[1]) 
     ],                     
     [ "(variantarrayint64)", 
        lambda :  dbus.Array(
                             [dbus.Int64( int64[0]), dbus.Int64( int64[1])]
                               , variant_level = 1), 
        lambda :"VARIANT(INT64({0}),INT64({1})))" .format (int64[0], int64[1]) 
     ], 
     [ "(variantarraydouble)", 
        lambda :  dbus.Array(
                             [dbus.Double( dbl[0]), dbus.Double( dbl[1] ) ]
                               , variant_level = 1), 
        lambda :"VARIANT(DOUBLE({0}),DOUBLE({1})))" .format (dbl[0], dbl[1]) 
     ], 
     [ "(variantarraybyte)", 
        lambda :  dbus.Array(
                             [dbus.Byte( b[0]), dbus.Byte( b[1]) ]
                               , variant_level = 1), 
        lambda :"VARIANT(BYTE({0}),BYTE({1})))" .format (b[0], b[1]) 
     ], 
     [ "(variantarrayboolean)", 
        lambda :  dbus.Array(
                             [dbus.Boolean( bool[0]), dbus.Boolean( bool[1]) ] 
                               , variant_level = 1), 
        lambda :"VARIANT(BOOLEAN({0}),BOOLEAN({1})))" .format (bool[0], bool[1]) 
     ], 
     [ "(variantarrayobjpath)", 
        lambda :  dbus.Array(
                             [dbus.ObjectPath( "/" + s[0]), dbus.ObjectPath( "/" + s[1]) ]
                               , variant_level = 1), 
        lambda :"VARIANT(ARRAY(OBJECT_PATH(/{0}),OBJECT_PATH(/{1})))" .format (s[0], s[1]) 
     ], 

# struct
     [ "(byte(bytebytebyte)", 
        lambda :  dbus.Struct
                (
                    (
                        dbus.Byte(b[0]),
                        dbus.Struct
                        (
                            ( dbus.Byte(b[1]), dbus.Byte(b[2]), dbus.Byte(b[3]) )
                        )
                    )
                ), 
        lambda :"STRUCT(BYTE:%d,STRUCT(BYTE:%d,BYTE:%d,BYTE:%d)))" % (b[0], b[1], b[2], b[3]) 
     ],  
                   
     [ "(byte(bytebytebyte)uint32", 
        lambda :  dbus.Struct
                (
                    (
                        dbus.Byte(b[0]),
                        dbus.Struct
                        (
                            ( dbus.Byte(b[1]), dbus.Byte(b[2]), dbus.Byte(b[3]) )
                        ),
                        dbus.UInt32(uint32[0]),
                    )
                ), 
        lambda :"STRUCT(BYTE:%d,STRUCT(BYTE:%d,BYTE:%d,BYTE:%d)),UINT32:%d)" % (b[0], b[1], b[2], b[3], uint32[0]) 
     ],  

     [ "(stringbyte)", 
        lambda :  dbus.Struct
                (
                    (
                        dbus.String(s[0]),
                        dbus.Byte(b[0])
                    )
                ), 
        lambda :"STRUCT(STRING:%s,BYTE:%d)" % (s[0], b[0]) 
     ],

    [ "(stringbyte)", 
        lambda :  dbus.Struct
                (
                    (
                        dbus.String(s[0]),
                        dbus.Byte(b[0])
                    )
                ), 
        lambda :"STRUCT(STRING:%s,BYTE:%d)" % (s[0], b[0]) 
     ],
    [ "(stringuint16)", 
        lambda :  dbus.Struct
                (
                    (
                        dbus.String(s[0]),
                        dbus.UInt16(uint16[0])
                    )
                ), 
        lambda :"STRUCT(STRING:%s,UINT16:%d)" % (s[0], uint16[0]) 
     ], 
    [ "(stringuint32)", 
        lambda :  dbus.Struct
                (
                    (
                        dbus.String(s[0]),
                        dbus.UInt32(uint32[0])
                    )
                ), 
        lambda :"STRUCT(STRING:%s,UINT32:%d)" % (s[0], uint32[0]) 
     ], 
    [ "(stringuint64)", 
        lambda :  dbus.Struct
                (
                    (
                        dbus.String(s[0]),
                        dbus.UInt64(uint64[0])
                    )
                ), 
        lambda :"STRUCT(STRING:%s,UINT64:%lu)" % (s[0], uint64[0]) 
     ], 
 

# dict
    [ "{stringbyte}", 
        lambda :  dbus.Dictionary
                (
                    {
                        dbus.String(s[0]): dbus.Byte(b[0])
                    }
                ), 
        lambda :"DICT(STRING:%s,BYTE:%d)" % (s[0], b[0]) 
     ], 
                   
    [ "array{stringbyte}", 
        lambda :  dbus.Dictionary
                (
                    {
                        dbus.String(s[0]): dbus.Byte(b[0]),                   
                        dbus.String(s[1]): dbus.Byte(b[1])
                    }
                ), 
        lambda :"ARRAY(DICT(STRING:%s,BYTE:%d),DICT(STRING:%s,BYTE:%d))" % (s[0], b[0], s[1], b[1]) 
     ], 

    [ "array{stringuint16}", 
        lambda :  dbus.Dictionary
                (
                    {
                        dbus.String(s[0]): dbus.UInt16(uint16[0]),
                        
                        dbus.String(s[1]): dbus.UInt16(uint16[1])
                    }
                ), 
        lambda :"ARRAY(DICT(STRING:%s,UINT16:%d),DICT(STRING:%s,UINT16:%d))" % (s[0], uint16[0], s[1], uint16[1]) 
     ], 
    [ "array{stringuint32}", 
        lambda :  dbus.Dictionary
                (
                    {
                        dbus.String(s[0]): dbus.UInt32(uint32[0]), 
                        
                        dbus.String(s[1]): dbus.UInt32(uint32[1])
                    }
                ), 
        lambda :"ARRAY(DICT(STRING:%s,UINT32:%d),DICT(STRING:%s,UINT32:%d))" % (s[0], uint32[0], s[1], uint32[1]) 
     ], 
    [ "array{stringuint64}", 
        lambda :  dbus.Dictionary
                (
                    {
                        dbus.String(s[0]): dbus.UInt64(uint64[0]), 
                        
                        dbus.String(s[1]):dbus.UInt64(uint64[1])
                    }
                ), 
        lambda :"ARRAY(DICT(STRING:%s,UINT64:%d),DICT(STRING:%s,UINT64:%d))" % (s[0], uint64[0], s[1], uint64[1]) 
     ], 
    [ "array{stringuint64}", 
        lambda :  dbus.Dictionary
                (
                    {
                        dbus.UInt64(uint64[0]): 
                        dbus.Struct(
                                    (dbus.String(s[0]), dbus.String(s[1]), dbus.String(s[2]))
                                    ),
                     
                        dbus.UInt64(uint64[1]):
                        dbus.Struct(
                                    (dbus.String(s[3]), dbus.String(s[4]), dbus.String(s[5]))
                                    ) 
                    }
                ), 
        lambda :"ARRAY(DICT(UINT64:%d,STRUCT(STRING:%s,STRING:%s,STRING:%s)),DICT(UINT64:%d,STRUCT(STRING:%s,STRING:%s,STRING:%s)))" 
        % ( uint64[0], s[0], s[1], s[2], uint64[1], s[3], s[4], s[5]) 
     ], 
     
    [ "array{stringvariantarraystructstringuint32}", 
        lambda :  dbus.Dictionary
                (
                    {
                        dbus.String(s[0]): 
                            dbus.Array( 
                                (dbus.Struct(
                                    (dbus.String(s[1]),
                                    dbus.UInt32(uint32[0]))
                                    ),
                                dbus.Struct(
                                    (dbus.String(s[2]),
                                    dbus.UInt32(uint32[1])),
                                    ),
                                dbus.Struct(
                                    (dbus.String(s[3]),
                                    dbus.UInt32(uint32[2]))
                                    ))
                                , variant_level = 1),

                        dbus.String(s[4]): 
                            dbus.Array( 
                                (dbus.Struct(
                                    (dbus.String(s[5]),
                                    dbus.UInt32(uint32[3]))
                                    ),
                                dbus.Struct(
                                    (dbus.String(s[6]),
                                    dbus.UInt32(uint32[4]))
                                    ),
                                dbus.Struct(
                                    (dbus.String(s[7]),
                                    dbus.UInt32(uint32[5]))
                                    )),
                            variant_level = 1),
                            
                        dbus.String(s[8]): 
                            dbus.Array( 
                                (dbus.Struct(
                                    (dbus.String(s[9]),
                                    dbus.UInt32(uint32[6]))
                                    ),
                                dbus.Struct(
                                    (dbus.String(s[10]),
                                    dbus.UInt32(uint32[7]))
                                    ),
                                dbus.Struct(
                                    (dbus.String(s[11]),
                                    dbus.UInt32(uint32[8]))
                                    )),
                            variant_level = 1)
                    }
                ), 
               
                
        lambda :"ARRAY("
                    "DICT("
                        "STRING:{0},VARIANT("
                                            "ARRAY("
                                                    "STRUCT("
                                                            "STRING:{1},UINT32:{2}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{3},UINT32:{4}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{5},UINT32:{6}"
                                                            ")"
                                                    ")"
                                           ")"
                           "),"
                       "DICT("
                        "STRING:{7},VARIANT("
                                            "ARRAY("
                                                    "STRUCT("
                                                            "STRING:{8},UINT32:{9}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{10},UINT32:{11}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{12},UINT32:{13}"
                                                            ")"
                                                    ")"
                                           ")"
                           "),"
                        "DICT("
                        "STRING:{14},VARIANT("
                                            "ARRAY("
                                                    "STRUCT("
                                                            "STRING:{15},UINT32:{16}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{17},UINT32:{18}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{19},UINT32:{20}"
                                                            ")"
                                                    ")"
                                           ")"
                           "))" . format (s[0], s[1], uint32[0], s[2], uint32[1], s[3], uint32[2], 
                           s[4], s[5], uint32[3], s[6], uint32[4], s[7], uint32[5], s[8], 
                           s[9], uint32[6], s[10], uint32[7], s[11], uint32[8])
     ], 
     
#null size
    [ "null string", 
        lambda :  dbus.Struct((dbus.String(""), dbus.String(""))), 
        lambda :"STRUCT(STRING:,STRING:)"  
    ], 
    [ "array{NULLstringvariantarraystructstringuint32}", 
        lambda :  dbus.Dictionary
                (
                    {
                        dbus.String(s[0]): 
                            dbus.Array( 
                                (
                                    dbus.Struct(
                                            (dbus.String(""),
                                            dbus.UInt32(uint32[0])
                                            )
                                        ),
                                    dbus.Struct(
                                            (dbus.String(""),
                                            dbus.UInt32(uint32[1])
                                            ),
                                        )
                                 )
                                , variant_level = 1),

                            
                        dbus.String(""): 
                            dbus.Array( dbus.Struct((dbus.UInt32(uint32[2]),dbus.UInt32(uint32[3]))),
                            variant_level = 1)
                    }
                ), 
               
                
        lambda :"ARRAY("
                    "DICT("
                        "STRING:{0},VARIANT("
                                            "ARRAY("
                                                    "STRUCT("
                                                            "STRING:{1},UINT32:{2}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{3},UINT32:{4}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{5},UINT32:{6}"
                                                            ")"
                                                    ")"
                                           ")"
                           "),"
                       "DICT("
                        "STRING:{7},VARIANT("
                                            "ARRAY("
                                                    "STRUCT("
                                                            "STRING:{8},UINT32:{9}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{10},UINT32:{11}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{12},UINT32:{13}"
                                                            ")"
                                                    ")"
                                           ")"
                           "),"
                        "DICT("
                        "STRING:{14},VARIANT("
                                            "ARRAY("
                                                    "STRUCT("
                                                            "STRING:{15},UINT32:{16}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{17},UINT32:{18}"
                                                            "),"
                                                    "STRUCT("
                                                            "STRING:{19},UINT32:{20}"
                                                            ")"
                                                    ")"
                                           ")"
                           "))" . format (s[0], s[1], uint32[0], s[2], uint32[1], s[3], uint32[2], 
                           s[4], s[5], uint32[3], s[6], uint32[4], s[7], uint32[5], s[8], 
                           s[9], uint32[6], s[10], uint32[7], s[11], uint32[8])
     ], 
            
    []
]


BUS='com.ydbus.sigtest'
PATH='/com/ydbus/sigtest'
IFACE='com.ydbus.sigtest'

KEY_SIG_IDX = 0
KEY_PARAM_SEND_IDX = 1
KEY_PARAM_STR_EXPECT_IDX = 2
KEY_PARAM_TEST_RESULT_BOOL_IDX = 3
KEY_PARAM_TEST_RESULT_STR_IDX = 4

def dbus_send(obj, param, method='sig_test_err1'):
    try:
#   dir(obj)
        obj.sig_test_err1(param);
    except:
        pass
        
def get_sigtest_obj():
    bus = dbus.SystemBus()
    sigtest = bus.get_object(BUS, PATH)
    return sigtest

def gen_random_string(rand_str_len):
    str = ''
    c="qwertyuiopasdfghjklmnbvcxzQWERTYUIOPLKJHGFDSAZXCVBNM1234567890"
    for i in range(0, rand_str_len):    
        str += c[random.randint(0, len(c) -1 )]
         
    return str        
    
def gen_random_array_string(array_len, rand_str_len):
    a = []
    for i in range(array_len):
        a.append( gen_random_string(rand_str_len))
        
    return a

def gen_random_array_byte(array_len):
    a = []
    for i in range(array_len):
        a.append( random.randint(0x20, 0x7e) )
        
    return a

def gen_random_array_bool(array_len):
    a = []
    for i in range(array_len):
        a.append( random.randint(0, 1) )
        
    return a


def gen_random_array_uint16(array_len):
    a = []
    for i in range(array_len):
        a.append( random.randint(0, 2**16-1) )
        
    return a

def gen_random_array_int16(array_len):
    a = []
    for i in range(array_len):
        a.append( random.randint(-2**15, 2**15-1) )
        
    return a

def gen_random_array_int32(array_len):
    a = []
    for i in range(array_len):
        a.append( random.randint(-2**31, 2**31-1) )
        
    return a

def gen_random_array_uint32(array_len):
    a = []
    for i in range(array_len):
        a.append( random.randint(0, 2**32-1) )
        
    return a
    
def gen_random_array_uint64(array_len):
    a = []
    for i in range(array_len):
        a.append( random.randint(0, 2**64-1) )
        
    return a

def gen_random_array_int64(array_len):
    a = []
    for i in range(array_len):
        a.append( random.randint(-2**31, 2**31-1) )
        
    return a
    
def gen_random_array_double(array_len):
    a = []
    for i in range(array_len):
        a.append( random.random() * random.randint(-2**31, 2**31-1) )
        
    return a

def gen_random_data():
    s = gen_random_array_string(20, random.randint(10,15))
    b = gen_random_array_byte(20)
    bool = gen_random_array_bool(20)
    uint16 = gen_random_array_uint16(20)
    int16 = gen_random_array_int16(20)
    uint32 = gen_random_array_uint32(20)
    int32 = gen_random_array_int32(20)
    uint64 = gen_random_array_uint64(20)
    int64 = gen_random_array_int64(20)    
    dbl = gen_random_array_double(20)
    
    return (s,b,bool, uint16,int16, uint32,int32,int64,uint64,dbl)

(s,b,bool, uint16,int16, uint32,int32,int64,uint64,dbl) = gen_random_data()




def clear_test_result(test_result):
    for i in range(0, len(DBUS_TEST_SUITE)):
        test_case = DBUS_TEST_SUITE[i]
        test_case[KEY_PARAM_SEND_IDX] = test_case[KEY_PARAM_SEND_IDX]() 
        test_case[KEY_PARAM_STR_EXPECT_IDX] = test_case[KEY_PARAM_STR_EXPECT_IDX]()
        test_case.append(False);
        test_case.append("");
        
        test_result.append(test_case)

def execute_test_case(sigobj, test_case_file_out):
    global s, b, bool, uint16, int16, int32, uint32, int64, uint64, dbl
    
    f = open(test_case_file_out,'w')
    
    for i in range(0, len(DBUS_TEST_SUITE) -1 ):
        
        (s,b,bool, uint16, int16, uint32,int32,int64,uint64,dbl) = gen_random_data()
        
        test_case = DBUS_TEST_SUITE[i]
        print '======================================'
        print 'Execute test case #%d, sig=%s' % (i, test_case[KEY_SIG_IDX])
        print "Sending '{0}' expect '{1}'".format(test_case[KEY_PARAM_SEND_IDX](), test_case[KEY_PARAM_STR_EXPECT_IDX]() )
        dbus_send(sigobj, test_case[KEY_PARAM_SEND_IDX]())
        #time.sleep(0.2)
        f.write("{0} {1} {2}\n" .format (i, test_case[KEY_SIG_IDX],  test_case[KEY_PARAM_STR_EXPECT_IDX]()))
        
        
    f.close()
    


def verify_test_result(test_case_file_out, test_file_log_in, test_result_out):
    test_case = open(test_case_file_out, 'r')
    test_log = open(test_file_log_in, 'r')
    test_result = open(test_result_out, 'w')
    for test_case_line in test_case:
        (test_case_idx, sig, expected_result) = test_case_line.split(" ")
        expected_result = expected_result.rstrip(os.linesep)
        
        for test_log_line in test_log:
            test_log_line = test_log_line.rstrip(os.linesep)
            
            if expected_result in test_log_line:
                test_result.write("%s '%s' '%s' '%s' ==> PASSED \n" % (test_case_idx, sig, expected_result, test_log_line) )
                test_log.seek(0)
                break
            
        if test_log_line is None:
            test_log.seek(0)
            test_result.write("%s '%s' '%s' ==> FAILED \n" % (test_case_idx, sig, expected_result) )
        
    test_case.close()
    test_log.close()
    test_result.close()
            

is_execute_all = sys.argv[1] == "-e"
is_verify_result = sys.argv[1] == "-v"
test_case_file_out = sys.argv[2]
if is_verify_result:
    test_file_log_in = sys.argv[3]
    test_result_out = sys.argv[4]

if is_execute_all:
    sig_test_obj = get_sigtest_obj()
    execute_test_case(sig_test_obj, test_case_file_out)
elif is_verify_result:
    verify_test_result(test_case_file_out, test_file_log_in, test_result_out)