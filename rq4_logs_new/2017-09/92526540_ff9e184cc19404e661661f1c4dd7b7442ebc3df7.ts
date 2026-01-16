import * as _ from 'lodash'
import {flatten} from 'flat'
import PubSub from '@google-cloud/pubsub'
import * as functions from 'firebase-functions'
import * as admin from 'firebase-admin'
import * as GCS from '@google-cloud/storage'
import * as express from 'express'

// Initialize firebase admin using functions login
admin.initializeApp(functions.config().firebase)


export const roleAddNewPermission = functions.database.ref('/roles/{roleId}/permissions/{permission}')
  .onCreate(async event => {
    let groupId, role: string
    [groupId, role] = [
      (
        await event.data.adminRef.parent!.parent!.child('group_id').once('value')
      ).val(),
      (
        await event.data.adminRef.root.child(`/roles/${event.params!.roleId}`).once('value')
      ).val()
    ]
    const groupMembersRef = event.data.adminRef.root.child(`/groups/${groupId}/members`)
    const groupMembers = (
      await groupMembersRef.once('value')
    ).val()

    const roleMembers = Object.keys(_.get(role, 'members', {}))

    Object.keys(_.pick(groupMembers, roleMembers))
      .map(memberId => {
        _.set(groupMembers, `${memberId}.permissions.${event.params!.permission}`, {
            ..._.get(groupMembers, `${memberId}.permissions.${event.params!.permission}`, {}),
            [event.params!.roleId]: true
          }
        )
      })
    console.log(groupMembers)
    await groupMembersRef.set(groupMembers)
  })


export const roleGrantNewMemberPermissions = functions.database.ref('/roles/{roleId}/members/{memberId}')
  .onCreate(async event => {
    let groupId, role: string
    [groupId, role] = [
      (
        await event.data.adminRef.parent!.parent!.child('group_id').once('value')
      ).val(),
      (
        await event.data.adminRef.root.child(`/roles/${event.params!.roleId}`).once('value')
      ).val()
    ]
    const groupMemberRef = event.data.adminRef.root.child(`/groups/${groupId}/members/${event.params!.memberId}`)
    const groupMemberSnapshot = (
      await groupMemberRef.once('value')
    ).val()

    const groupMember = _.isObject(groupMemberSnapshot.val()) ? groupMemberSnapshot.val() : {}

    console.log(groupMember)
    Object.keys(
      _.get(role, 'permissions', {})
    ).map((permission) => {
      _.set(groupMember, `permissions.${permission}`, {
          ..._.get(groupMember, `permissions.${permission}`, {}),
          [event.params!.roleId]: true
        }
      )
    })
    console.log(groupMember)
    await groupMemberRef.set(groupMember)
  })


export const roleRemovePermission = functions.database.ref('/roles/{roleId}/permissions/{permission}')
  .onDelete(async event => {
    const groupId = (
      await event.data.adminRef.parent!.parent!.child('group_id').once('value')
    ).val()

    const groupMembersRef = event.data.adminRef.root.child(`/groups/${groupId}/members`)

    const groupMembers = (
      await groupMembersRef.once('value')
    ).val()

    Object.keys(groupMembers)
      .map(memberId => {
        _.set(groupMembers, `${memberId}.permissions.${event.params!.permission}`, {
            ..._.get(groupMembers, `${memberId}.permissions.${event.params!.permission}`, {}),
            [event.params!.roleId]: null
          }
        )
      })
    console.log(groupMembers)
    await groupMembersRef.set(groupMembers)
  })


export const roleRevokeDeletedMemberPermissions = functions.database.ref('/roles/{roleId}/members/{memberId}')
  .onDelete(async event => {
    let groupId
    let role

    [groupId, role] = [
      (
        await event.data.adminRef.parent!.parent!.child('group_id').once('value')
      ).val(),
      (
        await event.data.adminRef.root.child(`/roles/${event.params!.roleId}`).once('value')
      ).val()
    ]

    const groupMemberRef = event.data.adminRef.root.child(`/groups/${groupId}/members/${event.params!.memberId}`)

    const groupMember = (
      await groupMemberRef.once('value')
    ).val()

    Object.keys(
      _.get(role, 'permissions', {})
    ).map((permission) => {
      _.set(groupMember, `permissions.${permission}`, {
          ..._.get(groupMember, `permissions.${permission}`, {}),
          [event.params!.roleId]: null
        }
      )
    })
    console.log(groupMember)
    await _.isEqual(_.omitBy(flatten(groupMember), _.isEmpty), {}) ?
      groupMemberRef.set(true) :
      groupMemberRef.set(groupMember)
  })


export const rawVideoNotifier = functions.storage.object().onChange(event => {
  if (event.data.name && event.data.name.startsWith('raw-videos/')) {
    console.log('Raw video to be encoded')
    const pubSub = PubSub()
    console.log(pubSub)
    pubSub.topic('raw-videos')
      .get(
        {autoCreate: true},
        function (err, topic, apiResponse) {
          console.log(apiResponse)
          topic.publish(event.data, function (err, messageIds, apiResponse) {
            console.log(messageIds)
            console.log(apiResponse)
          })
        }
      )
  }
})


export const videoNotifier = functions.storage.object().onChange(event => {
  if (event.data.name && event.data.name.startsWith('videos/')) {
    console.log('A video to be added')
  }
})


// A passthrough service to get files from google storage and serve them with CORS
const videoRouter = express()
videoRouter.get('/:videoId/:fileName', async (req, resp) => {
  resp.header('Access-Control-Allow-Origin', '*')
  const gcs = GCS()
  const bucket = gcs.bucket('iotv-1e541.appspot.com')
  const fileName = `videos/${req.params.videoId}/${req.params.fileName}`
  const file = bucket.file(fileName)
  try {
    const exists = await file.exists()
    if (exists) {
      resp.status(200)
      file.createReadStream().pipe(resp)
    } else {
      resp.status(404).send('Not Found')
    }
  } catch (err) {
    resp.status(500).send('Server Error')
  }
})

// Default 404 handler to quickly end bad routes
videoRouter.use((req, resp) => resp.sendStatus(404))

export const videos = functions.https.onRequest(videoRouter)