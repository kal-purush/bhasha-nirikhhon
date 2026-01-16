import * as _ from 'lodash';
import { Meteor } from 'meteor/meteor';
import { Jobs, RunConfig, RunArguments } from 'meteor/msavin:sjobs';
import * as moment from 'meteor/momentjs:moment';

import StravaProviderDAO from '../providers/strava';
import { Collection as ActivitiesCollection } from '../../imports/models/activities';

const SYNC_USER_ACTIVITIES_PRIROITY = 50;
const SYNC_USER_ACTIVITIES_RUN_CONFIG: RunConfig = {
  in: {
    minutes: 5,
  },
  priority: SYNC_USER_ACTIVITIES_PRIROITY,
};

export const SYNC_USER_ACTIVITIES_JOB_ID = 'syncUserActivties';

async function syncUserActivities(_args?: RunArguments) {
  console.info(`Running syncUserActivities`);
  const users = Meteor.users
    .find({
      $or: [
        {
          lastSyncedAt: {
            $lt: moment()
              .subtract(10, 'minutes')
              .toDate(),
          },
        },
        {
          lastSyncedAt: undefined,
        },
      ],
    })
    .fetch();

  console.info(`Found ${users.length} users to sync.`);

  for (const user of users) {
    const dao = new StravaProviderDAO(user);

    // TODO: Maybe we should limit this call to a certain number of activities?
    // TODO: Obey Strava rate limiting.
    const activities = await dao.getActivities();

    console.info(
      `Syncing ${activities.length} activities for user ${user._id}.`,
    );

    await Promise.all(
      _.map(activities, async activity => {
        await ActivitiesCollection.upsert(
          { provider: activity.provider, providerId: activity.providerId },
          { $set: activity },
        );
      }),
    );

    console.info(`Upadting lastSyncedAt for ${user._id}.`);

    Meteor.users.update({ _id: user._id }, { lastSyncedAt: new Date() });
  }

  // Run the job again, so it keeps looping.
  runJob();
}
console.info(`Registering ${SYNC_USER_ACTIVITIES_JOB_ID} job.`);
Jobs.register({
  SYNC_USER_ACTIVITIES_JOB_ID: syncUserActivities,
});

function runJob() {
  console.info(`Running ${SYNC_USER_ACTIVITIES_JOB_ID} job.`);
  Jobs.run(SYNC_USER_ACTIVITIES_JOB_ID, null, SYNC_USER_ACTIVITIES_RUN_CONFIG);
}