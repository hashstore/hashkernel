import codecs
import fnmatch
import logging
import os
from collections import defaultdict
from functools import total_ordering
from pathlib import Path
from typing import List, Sequence, Union

from hashkernel import reraise_with_msg
from hashkernel.files import aio_read_text, ensure_path, read_text

log = logging.getLogger(__name__)


@total_ordering
class PathMatch:
    """
    >>> abc_txt = PathMatch('a/b/c','*.txt')
    >>> ab_log = PathMatch('a/b','*.log')
    >>> abc_txt.match('a/b/c/d.txt')
    True
    >>> ab_log.match('a/b/c/d.log')
    True
    >>> ab_log == abc_txt
    False
    >>> PathMatch('a/b/','c/*.txt').match('a/b/c/d.txt')
    True
    >>> PathMatch('a/b/','c/*.txt').match('a/b/c2/d.txt')
    False
    >>> PathMatch('a/b/','c/*/').match('a/b/c/d')
    True
    >>> PathMatch('a/b/','c/*/').match('q/b/c/d')
    False
    >>> list(sorted([abc_txt, ab_log, abc_txt]))
    [PathMatch('a/b', '*.log'), PathMatch('a/b/c', '*.txt'), PathMatch('a/b/c', '*.txt')]

    """

    def __init__(self, cur_dir, pattern):
        self.root = ensure_path(cur_dir)
        self.pattern = pattern

    def match(self, path):
        path = ensure_path(path)
        if self.root in path.parents:
            rel_path = path.relative_to(self.root)
            return rel_path.match(self.pattern)
        return False

    def __key__(self):
        return (self.pattern, self.root)

    def __repr__(self):
        return f"PathMatch({str(self.root)!r}, {self.pattern!r})"

    def __lt__(self, other):
        return self.__key__() < other.__key__()

    def __eq__(self, other):
        return self.__key__() == other.__key__()

    def __hash__(self):
        return hash(self.__key__())

    def is_included(self, other):
        return self.pattern == other.pattern and self.root in other.root.parents


class PathMatchSet:
    """
    >>> pms = PathMatchSet()
    >>> pms.match('a/b/c/d.txt')
    False
    >>> pms.add(PathMatch('a/b/c', '*.txt'))
    True
    >>> pms.match('a/b/c/d.log')
    False
    >>> pms.add(PathMatch('a/b', '*.log'))
    True
    >>> pms.add(PathMatch('a/b/c', '*.txt'))
    False
    >>> pms.add(PathMatch('a/b/c/q/f', '*.txt'))
    False
    >>> pms.add(PathMatch('r/b/c/q/f', '*.txt'))
    True
    >>> pms.match('a/b/c/d.log')
    True
    >>> pms.match('a/b/c/d.txt')
    True
    >>>

    """

    def __init__(self):
        self.match_by_pattern = defaultdict(set)
        self.all_matches = set()

    def add(self, path_match):
        if path_match not in self.all_matches:
            for c in self.match_by_pattern[path_match.pattern]:
                if c.is_included(path_match):
                    return False
            self.match_by_pattern[path_match.pattern].add(path_match)
            self.all_matches.add(path_match)
            return True
        return False

    def match(self, path):
        path = ensure_path(path)
        return any(pm.match(path) for pm in self.all_matches)


class IgnoreRuleSet:
    root: Path
    ignore_files: PathMatchSet
    spec_to_parse: PathMatchSet

    def __init__(self, path: Path):
        self.root = path
        self.ignore_files = PathMatchSet()
        self.spec_to_parse = PathMatchSet()

    def update_ignore_files(self, *args: Union[str, PathMatch]):
        added = 0
        for pm in args:
            if isinstance(pm, PathMatch):
                assert self.root == pm.root or self.root in pm.root.parents
                if self.ignore_files.add(pm):
                    added += 1
            else:
                if self.ignore_files.add(PathMatch(self.root, pm)):
                    added += 1
        return added

    def update_spec_to_parse(self, *args: str):
        added = 0
        for pm in args:
            if self.spec_to_parse.add(PathMatch(self.root, pm)):
                added += 1
        return added

    def parse_specs(self, listdir: List[Path]) -> int:
        """ Returns number of specs parsed """
        specs_parsed = 0
        for p in listdir:
            if self.spec_to_parse.match(p):
                read_text(p, self.parse_spec)
                specs_parsed += 1
        return specs_parsed

    def parse_spec(self, path: Path, text: str):
        dir = path.parent
        for l in text.split("\n"):
            l = l.strip()
            if l != "" and l[0] != "#":
                self.ignore_files.add(PathMatch(dir, l))

    def path_filter(self, path: Path):
        return not self.ignore_files.match(path)


class IgnoreFilePolicy:
    def __init__(self, ignore_files, spec_to_parse=()):
        self.ignore_files = ignore_files
        self.spec_to_parse = spec_to_parse

    def apply(self, path: Path) -> IgnoreRuleSet:
        rule_set = IgnoreRuleSet(path)
        if self.ignore_files:
            rule_set.update_ignore_files(*self.ignore_files)
        if self.spec_to_parse:
            rule_set.update_spec_to_parse(*self.spec_to_parse)
        return rule_set


INCLUSIVE_POLICY = IgnoreFilePolicy(ignore_files=(), spec_to_parse=())

DEFAULT_IGNORE_POLICY = IgnoreFilePolicy(
    ignore_files=(
        ".svn",
        ".git",
        ".DS_Store",
        ".vol",
        ".hotfiles.btree",
        ".ssh",
        ".hs_*",
        ".backup*",
        ".Spotlight*",
        "._*",
        ".Trash*",
    ),
    spec_to_parse=(".gitignore", ".ignore"),
)
