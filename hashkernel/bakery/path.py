import os
from datetime import datetime
from io import BytesIO
from pathlib import PurePosixPath
from typing import IO, Callable, Optional

from hashkernel import EnsureIt, Primitive, Stringable
from hashkernel.bakery import Cake
from hashkernel.file_types import file_types, guess_name
from hashkernel.smattr import SmAttr


class CakePath(Stringable, EnsureIt, Primitive):
    """
    >>> Cake.from_bytes(b'[["b.text"], [null]]')
    Cake('vuftdaoxIKQUbgoReI06d24DhhzO6IaFGVgwqtIFVEH0')
    >>> root = CakePath('/vuftdaoxIKQUbgoReI06d24DhhzO6IaFGVgwqtIFVEH0')
    >>> root
    CakePath('/vuftdaoxIKQUbgoReI06d24DhhzO6IaFGVgwqtIFVEH0/')
    >>> root.root
    Cake('vuftdaoxIKQUbgoReI06d24DhhzO6IaFGVgwqtIFVEH0')
    >>> absolute = CakePath('/vuftdaoxIKQUbgoReI06d24DhhzO6IaFGVgwqtIFVEH0/b.txt')
    >>> absolute
    CakePath('/vuftdaoxIKQUbgoReI06d24DhhzO6IaFGVgwqtIFVEH0/b.txt')
    >>> relative = CakePath('y/z')
    >>> relative
    CakePath('y/z')
    >>> relative.make_absolute(absolute)
    CakePath('/vuftdaoxIKQUbgoReI06d24DhhzO6IaFGVgwqtIFVEH0/b.txt/y/z')

    `make_absolute()` have no effect to path that already
    absolute

    >>> p0 = CakePath('/vuftdaoxIKQUbgoReI06d24DhhzO6IaFGVgwqtIFVEH0/r/f')
    >>> p0.make_absolute(absolute)
    CakePath('/vuftdaoxIKQUbgoReI06d24DhhzO6IaFGVgwqtIFVEH0/r/f')
    >>> p1 = p0.parent()
    >>> p2 = p1.parent()
    >>> p1
    CakePath('/vuftdaoxIKQUbgoReI06d24DhhzO6IaFGVgwqtIFVEH0/r')
    >>> p2
    CakePath('/vuftdaoxIKQUbgoReI06d24DhhzO6IaFGVgwqtIFVEH0/')
    >>> p2.parent()
    >>> p0.path_join()
    'r/f'
    >>> p1.path_join()
    'r'
    >>> p2.path_join()
    ''
    >>> str(CakePath('q/x/палка_в/колесе.bin'))
    'q/x/палка_в/колесе.bin'
    """

    def __init__(self, s, _root=None, _path=[]):
        if s is None:
            self.root = _root
            self.path = _path
        else:
            split = PurePosixPath(s).parts
            if len(split) > 0 and split[0] == "/":
                self.root = Cake(split[1])
                self.path = split[2:]
            else:
                self.root = None
                self.path = split

    def child(self, name):
        path = list(self.path)
        path.append(name)
        return CakePath(None, _path=path, _root=self.root)

    def parent(self):
        if self.relative() or len(self.path) == 0:
            return None
        return CakePath(None, _path=self.path[:-1], _root=self.root)

    def next_in_relative_path(self):
        if not self.relative():
            raise AssertionError("only can be applied to relative")
        l = len(self.path)
        reminder = None
        if l < 1:
            next = None
        else:
            next = self.path[0]
            if l > 1:
                reminder = CakePath(None, _path=self.path[1:])
        return next, reminder

    def relative(self):
        return self.root is None

    def is_root(self):
        return not self.relative() and len(self.path) == 0

    def make_absolute(self, current_cake_path):
        if self.relative():
            path = list(current_cake_path.path)
            path.extend(self.path)
            return CakePath(None, _root=current_cake_path.root, _path=path)
        else:
            return self

    def __str__(self):
        if self.relative():
            return self.path_join()
        else:
            return f"/{str(self.root)}/{self.path_join()}"

    def path_join(self):
        return "/".join(self.path)

    def filename(self):
        l = len(self.path)
        if l > 0 and self.path[l - 1]:
            return self.path[l - 1]


def cake_or_path(s, relative_to_root=False):
    if isinstance(s, Cake) or isinstance(s, CakePath):
        return s
    elif s[:1] == "/":
        return CakePath(s)
    elif relative_to_root and "/" in s:
        return CakePath("/" + s)
    else:
        return Cake(s)


def ensure_cakepath(s):
    if not (isinstance(s, (Cake, CakePath))):
        s = cake_or_path(s)
    if isinstance(s, Cake):
        return CakePath(None, _root=s)
    else:
        return s


"""
@guest
    def info(self):
    def login(self, email, passwd, client_id=None):
    def authorize(self, cake, pts):
    def get_info(self, cake_path) -> PathInfo
    def get_content(self, cake_or_path) -> LookupInfo:

@user
    def logout(self):
    def write_content(self, fp, chunk_size=65355):
    def store_directories(self, directories):
    def create_portal(self, portal_id, cake):
    def edit_portal(self, portal_id, cake):
    def list_acls(self):
    def edit_portal_tree(self, files, asof_dt=None):
    def delete_in_portal_tree(self, cake_path, asof_dt = None):
    def get_portal_tree(self, portal_id, asof_dt=None):
    def grant_portal(self, portal_id, grantee, permission_type):
    def delete_portal(self, portal_id):

@admin
    def add_user(self, email, ssha_pwd, full_name = None):
    def remove_user(self, user_or_email):
    def add_acl(self, user_or_email, acl):
    def remove_acl(self, user_or_email, acl):

"""


# @abc.abstractmethod
# def store_directories(self, directories:Dict[Cake,CakeRack]):
#     raise NotImplementedError('subclasses must override')
# @abc.abstractmethod
# def get_info(self, cake_path: CakePath) -> "PathInfo":
#     raise NotImplementedError("subclasses must override")
#
# @abc.abstractmethod
# def get_content(self, cake_path: CakePath) -> "LookupInfo":
#     raise NotImplementedError("subclasses must override")

#
# @abc.abstractmethod
# def query(self, cake_path, include_path_info:bool=False) -> CakeRack:
#     raise NotImplementedError('subclasses must override')
#
# @abc.abstractmethod
# def edit_portal_tree(self,
#         files:List[PatchAction,Cake,Optional[Cake]],
#         asof_dt:datetime=None):
#     raise NotImplementedError('subclasses must override')


class PathResolved(SmAttr):
    path: CakePath
    resolved: Optional[Cake]


class PathInfo(SmAttr):
    mime: str
    file_type: Optional[str]
    created_dt: Optional[datetime]
    size: Optional[int]


class Content(PathInfo):
    __serialize_as__ = PathInfo
    data: Optional[bytes]
    stream_fn: Optional[Callable[[], IO[bytes]]]
    file: Optional[str]

    def has_data(self) -> bool:
        return self.data is not None

    def get_data(self) -> bytes:
        return self.stream().read() if self.data is None else self.data

    def stream(self) -> IO[bytes]:
        """
        stream is always avalable
        """
        if self.data is not None:
            return BytesIO(self.data)
        elif self.file is not None:
            return open(self.file, "rb")
        elif self.stream_fn is not None:
            return self.stream_fn()
        else:
            raise AssertionError(f"cannot stream: {repr(self)}")

    def has_file(self):
        return self.file is not None

    def open_fd(self):
        return os.open(self.file, os.O_RDONLY)

    @classmethod
    def from_file(cls, file):
        if not (os.path.exists(file)):
            raise FileNotFoundError()
        file_type = guess_name(file)
        return cls(file=file, file_type=file_type, mime=file_types[file_type].mime)

    @classmethod
    def from_data_and_file_type(
        cls, file_type: str, data: Optional[bytes] = None, file: Optional[str] = None
    ):
        if data is not None:
            return cls(
                data=data,
                size=len(data),
                file_type=file_type,
                mime=file_types[file_type].mime,
            )
        elif file is not None:
            return cls(file=file, file_type=file_type, mime=file_types[file_type].mime)
        else:
            raise AssertionError("file or data should be defined")


class LookupInfo(Content):
    path: CakePath
    info: Optional[PathInfo]


class RemoteError(ValueError):
    pass


class CredentialsError(ValueError):
    pass


class NotAuthorizedError(ValueError):
    pass


class NotFoundError(ValueError):
    pass


RESERVED_NAMES = ("_", "~", "-")


def check_bookmark_name(name):
    """
    >>> check_bookmark_name('a')
    >>> check_bookmark_name('_')
    Traceback (most recent call last):
    ...
    ValueError: Reserved name: _
    >>> check_bookmark_name('a/h')
    Traceback (most recent call last):
    ...
    ValueError: Cannot contain slash: a/h
    """
    if "/" in name:
        raise ValueError("Cannot contain slash: " + name)
    if name in RESERVED_NAMES:
        raise ValueError("Reserved name: " + name)
