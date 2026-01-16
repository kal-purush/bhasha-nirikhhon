import {getComment, getUrlFromComment} from '../src/utils'
import {RestEndpointMethodTypes} from '@octokit/plugin-rest-endpoint-methods/dist-types/generated/parameters-and-response-types'

describe('getComment', () => {
  it('should return undefined when there are no comments', () => {
    const comments: RestEndpointMethodTypes['issues']['listComments']['response']['data'] =
      []
    const pattern = ''
    const result = getComment({comments, pattern})
    expect(result).toBeUndefined()
  })
})

describe('getUrlFromComment', () => {
  it('should fallback when comment does not exist', () => {
    const body: any = undefined
    const result = getUrlFromComment(body)
    expect(result).toBeUndefined()
  })

  it('should fallback when comment has no body', () => {
    const result = getUrlFromComment({
      id: 0,
      node_id: '',
      url: '',
      html_url: '',
      user: {
        name: '',
        email: '',
        login: '',
        id: 0,
        node_id: '',
        avatar_url: '',
        gravatar_id: '',
        url: '',
        html_url: '',
        followers_url: '',
        following_url: '',
        gists_url: '',
        starred_url: '',
        subscriptions_url: '',
        organizations_url: '',
        repos_url: '',
        events_url: '',
        received_events_url: '',
        type: '',
        site_admin: false,
        starred_at: ''
      },
      created_at: '',
      updated_at: '',
      issue_url: '',
      author_association: 'COLLABORATOR'
    })
    expect(result).toBeUndefined()
  })
})