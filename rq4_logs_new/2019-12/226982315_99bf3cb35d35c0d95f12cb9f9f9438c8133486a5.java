/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clinicpatientregistration;

/**
 *
 * @author karis
 */

class PatientList {

       private PatientNode head;

       private int lastArrival;

       private int size;

       public PatientList() {

             head = null;

             lastArrival = 0;

             size = 0;

       }
       //String name, String age, String gender, String address, String phone, String id, int arrival, int priority
      //public void add(String name, int severity){
        public void add(String name, String age, String gender, String address, String phone, String id, int priority){

             Patient patient;

             lastArrival++;

             patient = new Patient(name,age,gender,address,phone,id, lastArrival, priority);

             // Check if size is zero

             if (size == 0)

             {
               head = new PatientNode(patient, null);
               size++;// count the size of nodes
             }

             else {

                    // Check severity of patient with the patient at head

                    int priority_p = head.data.checkSeverity(patient);

                    if (priority_p > 0) {

                           head = new PatientNode(patient, head);

                    } else {

                           boolean checkFirst = false;

                           // Get head node

                           PatientNode node = head;

                           while(!checkFirst && (node.next != null)) {

                                 priority_p = node.next.data.checkSeverity(patient);

                                 if (priority_p > 0) {

                                        PatientNode newPatientNode = new PatientNode(patient, node.next);

                                        node.next = newPatientNode;

                                        checkFirst = true;
                                 }

                                 node = node.next;

                           }

                           // If not added

                           if (!checkFirst) {

                                 node.next = new PatientNode(patient, null);

                           }

                    }

             }

          
       }

       public Patient nextAdmission() {

             PatientNode current;

             PatientNode previous;

             PatientNode toAdmitCurrent = null;

             PatientNode toAdmitPrevious = null;

             current = head;

             previous = null;

             while (current != null) {

                    if (toAdmitCurrent == null) {

                           toAdmitCurrent = current;

                    } else {

                           if (current.data.isAdmittedBefore(toAdmitCurrent.data,

                                        lastArrival)) {

                                 toAdmitCurrent = current;

                                 toAdmitPrevious = previous;

                           }

                    }

                    previous = current;

                    current = current.next;

             }

             if (toAdmitCurrent != null) {

                    if (toAdmitPrevious == null) {

                           head = toAdmitCurrent.next;

                    } else {

                           toAdmitPrevious.next = toAdmitCurrent.next;

                    }

                    size -= 1;

                    return toAdmitCurrent.data;

             } else {

                    return null;

             }

       }

       public int size() {

             return size;

       }

       public void print() {

             PatientNode current;

             current = head;

             while (current != null) {

                    System.out.println(current.data);

                    current = current.next;

             }

             System.out.println("Size = " + size);

             System.out.println("Last arrival = " + lastArrival);

       }

       public PatientList clone() {

             PatientList copy;

             PatientNode current;

             PatientNode copyCurrent;

             PatientNode newNode;

             copy = new PatientList();

             current = head;

             copyCurrent = null;

             while (current != null) {

                    newNode = new PatientNode(current.data, null);

                    if (copyCurrent == null) {

                           copy.head = newNode;

                    } else {

                           // last node in copy points to the new node

                           copyCurrent.next = newNode;

                    }

                    // move to the next node in both lists

                    copyCurrent = newNode;

                    current = current.next;

             }

             copy.lastArrival = lastArrival;

             copy.size = this.size;

             return copy;

       }
       
     

}