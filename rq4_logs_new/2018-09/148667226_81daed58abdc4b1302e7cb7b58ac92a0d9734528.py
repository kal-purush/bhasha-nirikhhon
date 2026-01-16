import typing as t
import mypy_extensions as mx


class ProfileDict(mx.TypedDict):
    token: str
    teamname: str
    username: str


class MetaDict(mx.TypedDict):
    next_page: t.Optional[str]
    previous_page: t.Optional[str]
    total: int


class TagDict(mx.TypedDict):
    name: str


class UserDict(mx.TypedDict):
    id: int
    name: str
    profile_image_url: str


class AttachmentDict(mx.TypedDict):
    name: str
    content: str


class AttachmentResultDict(mx.TypedDict):
    id: int
    name: str
    size: int
    url: str
    markdown: str
    created_at: str


class PostDict(mx.TypedDict):
    body: str
    comments: t.Sequence[t.Dict]  # untyped
    created_at: str
    draft: bool
    groups: t.Sequence[int]  # xxx
    id: int
    scope: str  # everyone,...
    tags: t.Sequence[TagDict]
    title: str
    url: str
    user: UserDict