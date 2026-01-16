import { CreatePostUseCase } from '../application/usecases/post/create-post.usecase';
import { PostTitle } from '../domain/valueObjects/post-title';
import { REPOSITORY_BINDINGS } from '../keys';
import { mockDiContainer } from '../mocks/container';
import type { IPostRepository } from '../mocks/mock-post-repository';

describe('CreatePostUseCase', () => {
  let useCase: CreatePostUseCase;
  let mockRepository: IPostRepository;

  beforeEach(() => {
    mockRepository = mockDiContainer.get<IPostRepository>(
      REPOSITORY_BINDINGS.PostRepository,
    );
    useCase = new CreatePostUseCase(mockRepository);
    // モックメソッドをリセット
    vi.clearAllMocks();
  });

  it('新しい投稿が作成されること', async () => {
    const createPostMock = vi
      .spyOn(mockRepository, 'createPost')
      .mockResolvedValue({ message: 'Post created successfully!' });
    const input = { title: 'Test Title', body: 'Test Body' };
    const result = await useCase.execute(input);
    expect(result).toEqual({ message: 'Post created successfully!' });
    expect(createPostMock).toHaveBeenCalledWith({
      id: 0,
      userId: 1,
      title: 'Test Title',
      body: 'Test Body',
    });
  });

  it('タイトルの文字数が36文字を超えている場合、エラーが返されること', async () => {
    const input = { title: 'a'.repeat(37), body: 'Test Body' };
    await expect(useCase.execute(input)).resolves.toEqual({
      message: PostTitle.lengthErrorMessage,
    });
  });
});