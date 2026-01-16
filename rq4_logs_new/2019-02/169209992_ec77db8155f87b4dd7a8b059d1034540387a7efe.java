package org.elaastic.qtapi.entities;

public class Assignement {

        String title
        User owner

        Date dateCreated
        Date lastUpdated

        String globalId

        static constraints = {
            title blank: false
            globalId nullable: true
        }

        /**
         * Get the sequences
         * @return the sequences
         */
        List<Sequence> getSequences() {
            Sequence.findAllByAssignment(this,[sort:'rank', order:'asc'])
        }

        Integer countSequences() {
            Sequence.countByAssignment(this)
        }

        /**
         * Get the last sequence of the current assignment
         * @return
         */
        Sequence getLastSequence() {
            def res = null
            if (sequences) {
                res = sequences.last()
            }
            res
        }

        static transients = ['sequences', 'lastSequence']

        /**
         * Count the number of registered users
         * @return the count
         */
        Integer registeredUserCount() {
            LearnerAssignment.countByAssignment(this)
        }
    }

}