import defaults from './defaults';
import { get, post, put, patch, del } from './util/requestAlias';
import { urlParamReplace } from './util/url';
import {
  AnswerRequestBody,
  CommentResponse,
  AnswerResponse,
  VoteCountResponse,
  VoteRequestBody,
  CommentRequestBody,
  UsersResponse,
  AnswersResponse,
  EmptyResponse,
  CommentsResponse,
} from './models';

interface DeleteParams {
  answerId: number;
}

interface AddVoteParams {
  answerId: number;
  voteRequestBody: VoteRequestBody;
}

interface CreateCommentParams {
  answerId: number;
  commentRequestBody: CommentRequestBody;
  include?: Array<string>;
}

interface DeleteMultipleParams {
  answerId?: Array<number>;
}

interface DeleteVoteParams {
  answerId: number;
}

interface DestroyParams {
  answerId: number;
}

interface DestroyMultipleParams {
  answerId?: Array<number>;
}

interface GetParams {
  answerId: number;
  include?: Array<string>;
}

interface GetCommentsParams {
  answerId: number;
  page?: number;
  perPage?: number;
  order?: string;
  include?: Array<string>;
}

interface GetDeletedParams {
  page?: number;
  perPage?: number;
  order?: string;
  answerId?: number;
  questionId?: number;
  userId?: number;
}

interface GetListParams {
  page?: number;
  perPage?: number;
  order?: string;
  include?: Array<string>;
  answerId?: number;
  questionId?: number;
  userId?: number;
}

interface GetVotersParams {
  answerId: number;
  page?: number;
  perPage?: number;
  include?: Array<string>;
  type?: 'up' | 'down';
}

interface RestoreParams {
  answerId: number;
}

interface RestoreMultipleParams {
  answerId?: Array<number>;
}

interface UpdateParams {
  answerId: number;
  answerRequestBody: AnswerRequestBody;
  include?: Array<string>;
}

/**
 * AnswerApi
 */
export default {
  /**
   * 删除指定回答
   * 只要没有错误异常，无论是否有回答被删除，该接口都会返回成功。  管理员可删除回答。回答作者是否可删除回答，由管理员在后台的设置决定。  回答被删除后，进入回收站。管理员可在后台恢复回答。
   * @param params.answerId 回答ID
   */
  del: (params: DeleteParams): Promise<EmptyResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace('AnswerApi.del', '/answers/{answer_id}', params, []);

    return del(url);
  },

  /**
   * 为回答投票
   * @param params.answerId 回答ID
   * @param params.voteRequestBody
   */
  addVote: (params: AddVoteParams): Promise<VoteCountResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace(
        'AnswerApi.addVote',
        '/answers/{answer_id}/voters',
        params,
        [],
      );

    return post(url, params.voteRequestBody || {});
  },

  /**
   * 在指定回答下发表评论
   * &#x60;include&#x60; 参数取值包括：&#x60;user&#x60;、&#x60;voting&#x60;
   * @param params.answerId 回答ID
   * @param params.commentRequestBody
   * @param params.include 包含的关联数据，用“,”分隔。
   */
  createComment: (params: CreateCommentParams): Promise<CommentResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace(
        'AnswerApi.createComment',
        '/answers/{answer_id}/comments',
        params,
        ['include'],
      );

    return post(url, params.commentRequestBody || {});
  },

  /**
   * 🔐批量删除回答
   * 只要没有错误异常，无论是否有回答被删除，该接口都会返回成功。  管理员可删除回答。回答作者是否可删除回答，由管理员在后台的设置决定。  回答被删除后，进入回收站。管理员可在后台恢复回答。
   * @param params.answerId 用“,”分隔的回答ID，最多可提供100个ID
   */
  deleteMultiple: (params: DeleteMultipleParams): Promise<EmptyResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace('AnswerApi.deleteMultiple', '/answers', params, [
        'answer_id',
      ]);

    return del(url);
  },

  /**
   * 取消为回答的投票
   * @param params.answerId 回答ID
   */
  deleteVote: (params: DeleteVoteParams): Promise<VoteCountResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace(
        'AnswerApi.deleteVote',
        '/answers/{answer_id}/voters',
        params,
        [],
      );

    return del(url);
  },

  /**
   * 🔐删除指定回答
   * 仅管理员可调用该接口。
   * @param params.answerId 回答ID
   */
  destroy: (params: DestroyParams): Promise<EmptyResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace(
        'AnswerApi.destroy',
        '/trash/answers/{answer_id}',
        params,
        [],
      );

    return del(url);
  },

  /**
   * 🔐批量删除回收站中的回答
   * 仅管理员可调用该接口  只要没有异常错误，无论是否有用户被禁用，该接口都会返回成功。  若没有提供 answer_id 参数，则将清空回收站中的所有回答。
   * @param params.answerId 用“,”分隔的回答ID，最多可提供100个ID
   */
  destroyMultiple: (params: DestroyMultipleParams): Promise<EmptyResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace('AnswerApi.destroyMultiple', '/trash/answers', params, [
        'answer_id',
      ]);

    return del(url);
  },

  /**
   * 获取回答详情
   * &#x60;include&#x60; 参数取值包括：&#x60;user&#x60;, &#x60;question&#x60;, &#x60;voting&#x60;
   * @param params.answerId 回答ID
   * @param params.include 包含的关联数据，用“,”分隔。
   */
  get: (params: GetParams): Promise<AnswerResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace('AnswerApi.get', '/answers/{answer_id}', params, [
        'include',
      ]);

    return get(url);
  },

  /**
   * 获取指定回答的评论
   * 可排序字段包括 &#x60;vote_count&#x60;、&#x60;create_time&#x60;，默认为 &#x60;create_time&#x60;  &#x60;include&#x60; 参数取值包括：&#x60;user&#x60;、&#x60;voting&#x60;
   * @param params.answerId 回答ID
   * @param params.page 当前页数
   * @param params.perPage 每页条数（最大为 100）
   * @param params.order 排序方式。在字段前加 &#x60;-&#x60; 表示倒序排列。例如 &#x60;create_time&#x60; 表示按时间顺序排列，&#x60;-create_time&#x60; 则表示按时间倒序排列。
   * @param params.include 包含的关联数据，用“,”分隔。
   */
  getComments: (params: GetCommentsParams): Promise<CommentsResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace(
        'AnswerApi.getComments',
        '/answers/{answer_id}/comments',
        params,
        ['page', 'per_page', 'order', 'include'],
      );

    return get(url);
  },

  /**
   * 🔐获取回收站中的回答列表
   * 仅管理员可调用该接口。 可排序字段包括 &#x60;vote_count&#x60;、&#x60;create_time&#x60;、&#x60;update_time&#x60;、&#x60;delete_time&#x60;，默认为 &#x60;-delete_time&#x60;
   * @param params.page 当前页数
   * @param params.perPage 每页条数（最大为 100）
   * @param params.order 排序方式。在字段前加 &#x60;-&#x60; 表示倒序排列。例如 &#x60;create_time&#x60; 表示按时间顺序排列，&#x60;-create_time&#x60; 则表示按时间倒序排列。
   * @param params.answerId 回答ID
   * @param params.questionId 提问ID
   * @param params.userId 用户ID
   */
  getDeleted: (params: GetDeletedParams): Promise<AnswersResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace('AnswerApi.getDeleted', '/trash/answers', params, [
        'page',
        'per_page',
        'order',
        'answer_id',
        'question_id',
        'user_id',
      ]);

    return get(url);
  },

  /**
   * 获取回答列表
   * 可排序字段包括 &#x60;vote_count&#x60;、&#x60;create_time&#x60;、&#x60;update_time&#x60;，默认为 &#x60;-create_time&#x60;  &#x60;include&#x60; 参数取值包括：&#x60;user&#x60;, &#x60;question&#x60;, &#x60;voting&#x60;
   * @param params.page 当前页数
   * @param params.perPage 每页条数（最大为 100）
   * @param params.order 排序方式。在字段前加 &#x60;-&#x60; 表示倒序排列。例如 &#x60;create_time&#x60; 表示按时间顺序排列，&#x60;-create_time&#x60; 则表示按时间倒序排列。
   * @param params.include 包含的关联数据，用“,”分隔。
   * @param params.answerId 回答ID
   * @param params.questionId 提问ID
   * @param params.userId 用户ID
   */
  getList: (params: GetListParams): Promise<AnswersResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace('AnswerApi.getList', '/answers', params, [
        'page',
        'per_page',
        'order',
        'include',
        'answer_id',
        'question_id',
        'user_id',
      ]);

    return get(url);
  },

  /**
   * 获取回答的投票者
   * &#x60;include&#x60; 参数取值包括：&#x60;is_followed&#x60;、&#x60;is_following&#x60;、&#x60;is_me&#x60;
   * @param params.answerId 回答ID
   * @param params.page 当前页数
   * @param params.perPage 每页条数（最大为 100）
   * @param params.include 包含的关联数据，用“,”分隔。
   * @param params.type 默认获取全部投票类型的用户 &#x60;up&#x60; 表示仅获取投赞成票的用户 &#x60;down&#x60; 表示仅获取投反对票的用户
   */
  getVoters: (params: GetVotersParams): Promise<UsersResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace(
        'AnswerApi.getVoters',
        '/answers/{answer_id}/voters',
        params,
        ['page', 'per_page', 'include', 'type'],
      );

    return get(url);
  },

  /**
   * 🔐恢复指定回答
   * 仅管理员可调用该接口。
   * @param params.answerId 回答ID
   */
  restore: (params: RestoreParams): Promise<AnswerResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace(
        'AnswerApi.restore',
        '/trash/answers/{answer_id}',
        params,
        [],
      );

    return post(url);
  },

  /**
   * 🔐批量恢复回答
   * 仅管理员可调用该接口。  只要没有异常错误，无论是否有用户被禁用，该接口都会返回成功。
   * @param params.answerId 用“,”分隔的回答ID，最多可提供100个ID
   */
  restoreMultiple: (params: RestoreMultipleParams): Promise<EmptyResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace('AnswerApi.restoreMultiple', '/trash/answers', params, [
        'answer_id',
      ]);

    return post(url);
  },

  /**
   * 修改回答信息
   * 管理员可修改回答。回答作者是否可修改回答，由管理员在后台的设置决定。  &#x60;content_markdown&#x60; 和 &#x60;content_rendered&#x60; 两个参数仅传入其中一个即可， 若两个参数都传入，则以 &#x60;content_markdown&#x60; 为准。  &#x60;include&#x60; 参数取值包括：&#x60;user&#x60;, &#x60;question&#x60;, &#x60;voting&#x60;
   * @param params.answerId 回答ID
   * @param params.answerRequestBody
   * @param params.include 包含的关联数据，用“,”分隔。
   */
  update: (params: UpdateParams): Promise<AnswerResponse> => {
    const url =
      defaults.apiPath +
      urlParamReplace('AnswerApi.update', '/answers/{answer_id}', params, [
        'include',
      ]);

    return patch(url, params.answerRequestBody || {});
  },
};