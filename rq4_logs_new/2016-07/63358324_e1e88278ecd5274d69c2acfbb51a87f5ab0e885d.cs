using System;
using System.Collections.Generic;
/* Group 3
 * Arlina Ramrattan, Neil Reading, Aaron Fernandes, Jay Clipperton
 * Assignment 4 - Ciphering Using Array List
*/
namespace COMP212Group3Assignment5
{
    // CIPHER CLASS
    class Cipher
    {
        // PRIVATE INSTANCE VARIABLES

        private LinkedList<char> _originalLinkList,_keyLinkList;

        // CONSTRUCTOR
        public Cipher()
        {
            // set array lists
            this._originalLinkList = new LinkedList<char>("abcdefghijklmnopqrstuvwxyz ".ToCharArray());
            this._keyLinkList = new LinkedList<char>("tmesfkjaxnovluchzgybwpdriq ".ToCharArray());
        }

        // PUBLIC METHODS
        public LinkedList<char> Encrypt(String data)
        {
            data = data.ToLower();
            int index;
            LinkedList<char> _decodedArrayList = new LinkedList<char>();
            foreach (Char c in data)
            {
                index=0;
                foreach(char ch in this._originalLinkList){
                    if(c==ch){
                        break;
                    }
                    index++;
                   
                }
                LinkedListNode<char> currentNode=this._keyLinkList.First;
                for(int i=0;i<=index;i++){
                    if(i==index){
                        break;
                    }
                    currentNode=currentNode.Next;
                }
                _decodedArrayList.AddLast(currentNode.Value);
            }
            return _decodedArrayList;
        }

        public LinkedList<char> Decrypt(String data)
        {
            data = data.ToLower();
            int index;
            LinkedList<char> _decodedArrayList = new LinkedList<char>();
            foreach (Char c in data)
            {
                index = 0;
                foreach (char ch in this._keyLinkList)
                {
                    if (c == ch)
                    {
                        break;
                    }
                    index++;

                }
                LinkedListNode<char> currentNode = this._originalLinkList.First;
                for (int i = 0; i <= index; i++)
                {
                    if (i == index)
                    {
                        break;
                    }
                    currentNode = currentNode.Next;
                }
                _decodedArrayList.AddLast(currentNode.Value);
            }
            return _decodedArrayList;
        }

    }
}