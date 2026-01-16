import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { Essay } from '.'
import { NexusGenRootTypes } from 'teachers-aid-server/src/teachers-aid-typegen'
import { ObjectId } from 'mongodb'
import { MarkingPeriodEnum } from '../../general'

export const UpdateEssaysByStudentsAndSectionInput = inputObjectType({
  name: 'UpdateEssaysByStudentsAndSectionInput',
  definition(t) {
    t.list.id('studentId', { required: true })
    t.string('section', { required: true })
    t.field('markingPeriod', { type: MarkingPeriodEnum })
    t.string('dueTime')
    t.date('assignedDate')
    t.date('dueDate')
    t.int('maxPoints')
  },
})

export const UpdateEssaysByStudentsAndSectionPayload = objectType({
  name: 'UpdateEssaysByStudentsAndSectionPayload',
  definition(t) {
    t.list.field('essays', { type: Essay })
  },
})

export const UpdateEssaysByStudentsAndSection = mutationField(
  'updateEssaysByStudentsAndSection',
  {
    type: UpdateEssaysByStudentsAndSectionPayload,
    description: 'For updating an assignment for an entire course(s)',
    args: {
      input: arg({
        type: UpdateEssaysByStudentsAndSectionInput,
        required: true,
      }),
    },
    async resolve(
      _,
      {
        input: {
          studentId,
          section,
          markingPeriod,
          dueTime,
          dueDate,
          assignedDate,
          maxPoints,
        },
      },
      { assignmentData }
    ) {
      const essays: NexusGenRootTypes['Essay'][] = []
      const studentIdsWithNoEssay: string[] = []

      for (const _id of studentId) {
        const essayValidation: NexusGenRootTypes['Essay'][] = await assignmentData.findOne(
          {
            'hasOwner._id': new ObjectId(_id),
            'readings.readingSections': section,
          }
        )
        if (essayValidation) {
          await assignmentData.updateOne(
            {
              'hasOwner._id': new ObjectId(_id),
              'readings.readingSections': section,
            },
            {
              $set: {
                markingPeriod,
                dueTime,
                dueDate,
                assignedDate,
                'score.maxPoints': maxPoints,
              },
            }
          )
        }
        const essay = await assignmentData.findOne({
          'hasOwner._id': new ObjectId(_id),
          'readings.readingSections': section,
        })
        essays.push(essay)
      }
      return { essays }

      // const essayIds = essayValidation.map((essay) => essay._id) as string[]
    },
  }
)