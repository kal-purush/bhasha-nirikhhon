import { gql, useMutation, useQuery } from '@apollo/client'
import { Announcement } from '../types/announcement'
import hasura from '../hasura'
import dayjs from 'dayjs'
import { PeriodType } from '../types/general'
import { useAuth } from 'lodestar-app-element/src/contexts/AuthContext'

export const useMemberAnnouncement = () => {
  const path = '/' + window.location.pathname.split('/admin')[1].split('/')[1]
  const { currentMemberId } = useAuth()
  const { data, loading, error, refetch } = useQuery<
    hasura.GetMemberAnnouncement,
    hasura.GetMemberAnnouncementVariables
  >(GetMemberAnnouncement, {
    variables: { memberId: currentMemberId || '', path },
    skip: !currentMemberId || !path,
  })

  const [upsertMemberAnnouncementStatus] = useMutation<
    hasura.UpsertMemberAnnouncementStatus,
    hasura.UpsertMemberAnnouncementStatusVariables
  >(UpsertMemberAnnouncementStatus)

  const announcements: Announcement[] =
    data?.announcement.map(announcement => ({
      id: announcement.id,
      appId: announcement.app_id,
      title: announcement.title,
      content: announcement.content || '',
      remindPeriodType: (announcement.remind_period_type as PeriodType) || 'D', // Default to 24 hours
      remindPeriodAmount: announcement.remind_period_amount || 1, // Default to 24 hours
      statedAt: announcement.started_at ? dayjs(announcement.started_at).toDate() : null,
      endedAt: announcement.ended_at ? dayjs(announcement.ended_at).toDate() : null,
      publishedAt: announcement.published_at ? dayjs(announcement.published_at).toDate() : null,
      isUniversalDisplay: announcement.is_universal_display,
      createdAt: dayjs(announcement.created_at).toDate(),
      updatedAt: dayjs(announcement.updated_at).toDate(),
      announcementPages: announcement.announcement_pages.map(page => ({
        id: page.id,
        announcementId: page.announcement_id,
        path: page.path,
        createdAt: dayjs(page.created_at).toDate(),
        updatedAt: dayjs(page.updated_at).toDate(),
      })),
      memberAnnouncementStatus: announcement.member_announcement_status.map(status => ({
        id: status.id,
        announcementId: status.announcement_id,
        memberId: status.member_id,
        readAt: status.read_at ? dayjs(status.read_at).toDate() : null,
        remindAt: status.remind_at ? dayjs(status.remind_at).toDate() : null,
        isDismissed: status.is_dismissed,
        createdAt: dayjs(status.created_at).toDate(),
        updatedAt: dayjs(status.updated_at).toDate(),
      })),
    })) || []

  return {
    loading,
    error,
    announcements,
    refetch,
    upsertMemberAnnouncementStatus,
  }
}

const GetMemberAnnouncement = gql`
  query GetMemberAnnouncement($memberId: String!, $path: String!) {
    announcement(
      where: {
        _and: [
          { published_at: { _is_null: false } }
          { _or: [{ ended_at: { _is_null: true } }, { ended_at: { _gte: "now()" } }] }
          { _or: [{ started_at: { _is_null: true } }, { started_at: { _lte: "now()" } }] }
          { _or: [{ is_universal_display: { _eq: true } }, { announcement_pages: { path: { _eq: $path } } }] }
        ]
      }
      order_by: { created_at: asc }
    ) {
      id
      app_id
      title
      content
      remind_period_type
      remind_period_amount
      started_at
      ended_at
      published_at
      is_universal_display
      created_at
      updated_at
      announcement_pages {
        id
        announcement_id
        path
        created_at
        updated_at
      }
      member_announcement_status(where: { member_id: { _eq: $memberId } }) {
        id
        announcement_id
        member_id
        read_at
        remind_at
        is_dismissed
        created_at
        updated_at
      }
    }
  }
`

const UpsertMemberAnnouncementStatus = gql`
  mutation UpsertMemberAnnouncementStatus(
    $memberId: String!
    $announcementId: uuid!
    $isDismissed: Boolean
    $remindAt: timestamptz
  ) {
    insert_member_announcement_status(
      objects: {
        announcement_id: $announcementId
        member_id: $memberId
        is_dismissed: $isDismissed
        remind_at: $remindAt
        read_at: "now()"
      }
      on_conflict: {
        constraint: member_announcement_status_announcement_id_member_id_key
        update_columns: [is_dismissed, read_at, remind_at]
      }
    ) {
      affected_rows
    }
  }
`