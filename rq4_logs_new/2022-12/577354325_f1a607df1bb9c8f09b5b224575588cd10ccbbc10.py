#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 Class Node for records with key_value information and pointer to next field
All the most common methods of a LinkedList:
Attach an item to the head of the LinkedList
attack at the end
print(linkedList)
delete value at index
insert value at index etc..
 
"""

class Node:
    def __init__(self,data=None,next=None):
        self.data=data
        self.next=next
        
class  LinkedList:
    def __init__(self) :
        # initially the emptyhead is passed two fields value and next
        self.head=Node()
        
        
    def insertATbeginning(self,data):
       # first method that adds an item to the head of the list
        node= Node(data,self.head)
        self.head=node
        
        
    def print(self):
        if self.head == None: return ' LinkedList is Empty'
        itr =self.head 
        string =' '
        # creating and empty string first
        while itr:
          ## in the iteration it returns all the values ​​of the linked list represented in the form of a string
            string += str(itr.data) +' --->  '
            itr=itr.next
        return string 
    
    def insert_atEnd(self,data):
              '''attach an item to the tail of the LinkedList'''
              if self.head is None : 
                 self.head=Node(data,None)
                 return 
              itr =self.head 
              while itr.next:
                     itr=itr.next 
              itr.next=Node(data,None)
              
    def insert_values(self,data_list):
        ''' builds a LInkedList starting from an input list and looping 
        on the function that attaches an element to the end of the LinkedList'''
        self.head=None
        for i in data_list:
            self.insert_atEnd(i)
        return self.print()
    


    def count_nodes(self):
        ''' counts the items in the list'''
        if self.head== None : return ' LinkedList is Empty'
        itr=self.head
        count=0
        while itr :
            count+=1
            itr= itr.next 
        return ' this is the length of the LinkedList ' +"  " +str(count)
    
    
    def remove_atIndex(self,index):
        ''' function that given an index if present removes the element that is in that index'''
        if index < 0 or str(index) >= self.count_nodes(): 
            #parte da zero index indi per cui non può essere uguale a self.length
            raise Exception('Invalid Index') # else Throws an Exception, because Index not found
            
        if index==0 :
            self.head=self.head.next 
            return 'this is the new head of the list'
        itr=self.head 
        count=0
        while itr:
            if index  == count +1: 
                 itr.next=itr.next.next
                 break
            count +=1
            itr=itr.next
        return self.print()
    
    
    
    def insertAt(self,index,data):
        ''' now I define a function that
        insert a new value into the index
        desired '''
        if index <0 or str(index) >= self.count_nodes():
            raise Exception('out of Lista')
        if index==0:
            self.insertATbeginning(data)
        
        itr=self.head 
        count=0
        while itr:
              if index-1==count:
                  # correct way since count starts from zero
                  node=Node(data,itr.next)
                  # that is, that the input data is created with the records of the class constructor
                  itr.next=node
                  break
              count+=1
              itr=itr.next
        return self.print()
                
            
        
        
    def append (self,data):
        if self.head==None: return None
        new_node=Node(data)
        cur=self.head
        while cur.next != None :
            cur=cur.next
        cur.next=new_node
        return self.print()
    
    
    

    
    def erease(self,index):
        ''' function that deletes an element at the requested index'''
        if index < 0 or str(index) >= self.count_nodes():
            raise Exception(' The index is out of range')
        occ=self.head
        count=0
        while True:
            last_occ=occ
            #I save the position where I am on a variable
            occ=occ.next
            if index-1==count:
                last_occ.next=occ.next
                # if I found the index to delete the last value pointer
                # before exiting the if condition hooks to the next of the next value
                return self.print()
            count+=1
        return
            
            
    

if __name__=='__main__':
     ll=LinkedList()
     ll.insert_atEnd(7)
     ll.insert_values(['Marocco ','try','worldcup','Victory'])
     ll.append('gipus')
     ll.append(45)
     ll.append(90)
     print(ll.print())
     