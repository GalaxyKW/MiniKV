#!/usr/bin/env python3
"""Check README.md and docs/**/*.md using Python 3.8+ and Bash, without execution.

This is a bounded repository check, not a Markdown renderer. It supports single-line
inline links/images (balanced parentheses, angle destinations, optional quoted
titles), relative paths, percent escapes, and ATX heading fragments. Headings may
contain plain text and inline code. Root-level fences use backticks or tildes with
up to three leading spaces. Only sh/bash/shell fences are passed to bash -n.

Reference links, HTML links/images, setext headings, container/indented fences,
and rich heading markup are unsupported and diagnosed. Multiline links and code
spans, indented code blocks, and arbitrary embedded HTML are outside this check's
scope. External URLs are skipped; command behavior and external reachability are
never tested. Use inline code or fences for literal Markdown examples.
"""

import argparse
from html.entities import html5
import os
from pathlib import Path
import re
import subprocess
import sys
import unicodedata
from urllib.parse import unquote, urlsplit


FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
HEADING = re.compile(r"^ {0,3}#{1,6}(?:[ \t]+(.*)|[ \t]*)$")
ESCAPE = re.compile(r"\\([!\"#$%&'()*+,\-./:;<=>?@\[\]^_`{|}~\\])")
MARKDOWN_TEXT = re.compile(
    ESCAPE.pattern + r"|&(?:#[0-9]{1,7}|#[xX][0-9a-fA-F]{1,6}|[A-Za-z][A-Za-z0-9]{1,31});")
TITLE = re.compile(r'''(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|\((?:\\.|[^)\\])*\))[ \t]*\)''')


def decode_text(text):
    """Decode Markdown escapes/entities once, without HTML's legacy recovery."""
    def replace(match):
        if match.group(1) is not None:
            return match.group(1)
        entity = match.group()
        if entity.startswith("&#"):
            digits = entity[2:-1]
            value = int(digits[1:], 16) if digits[0] in "xX" else int(digits)
            if value == 0 or value > 0x10ffff or 0xd800 <= value <= 0xdfff:
                return "\ufffd"
            return chr(value)
        return html5.get(entity[1:], entity)

    return MARKDOWN_TEXT.sub(replace, text)


def code_spans(line, keep_text=False):
    """Mask same-line code spans, or render heading text with literal code bodies."""
    def outside(text):
        return decode_text(text) if keep_text else text

    runs = list(re.finditer(r"`+", line))
    result, start, i = [], 0, 0
    while i < len(runs):
        opening = runs[i]
        prefix = line[:opening.start()]
        if (len(prefix) - len(prefix.rstrip("\\"))) % 2:
            i += 1
            continue
        end = next((j for j in range(i + 1, len(runs))
                    if runs[j].group() == opening.group()), None)
        if end is None:
            i += 1
            continue
        closing = runs[end]
        result.append(outside(line[start:opening.start()]))
        body = line[opening.end():closing.start()]
        if body.startswith(" ") and body.endswith(" ") and body.strip(" "):
            body = body[1:-1]
        result.append(body if keep_text else " " * (closing.end() - opening.start()))
        start, i = closing.end(), end + 1
    return "".join(result) + outside(line[start:])


def destination(line, start):
    """Return (raw destination, index after closing ')'); reject partial parses."""
    i = start
    while i < len(line) and line[i] in " \t":
        i += 1
    begin = i
    if i < len(line) and line[i] == "<":
        i += 1
        begin = i
        while i < len(line) and line[i] != ">":
            i += 2 if line[i] == "\\" else 1
        if i >= len(line):
            raise ValueError("unclosed angle link destination")
        raw = line[begin:i]
        i += 1
    else:
        depth = 0
        while i < len(line):
            char = line[i]
            if char == "\\":
                i += 2
                continue
            if char in " \t" or (char == ")" and depth == 0):
                break
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char in "<>":
                raise ValueError("use <...> around angle link destinations")
            i += 1
        raw = line[begin:i]
    while i < len(line) and line[i] in " \t":
        i += 1
    if i < len(line) and line[i] == ")":
        return raw, i + 1
    title = TITLE.match(line, i)
    if title:
        return raw, title.end()
    raise ValueError("unsupported or unclosed inline link; use a single-line destination")


class Checker:
    def __init__(self, root):
        self.root = root.resolve()
        self.errors = []
        self.documents = {}
        self.links = 0
        self.shell_blocks = 0

    def error(self, path, line, message):
        try:
            name = path.relative_to(self.root)
        except ValueError:
            name = path
        self.errors.append("{}:{}: {}".format(name, line, message))

    def document(self, path):
        if path in self.documents:
            return self.documents[path]
        visible, blocks, anchors = [], [], set()
        self.documents[path] = visible, blocks, anchors
        try:
            resolved = path.resolve()
            try:
                resolved.relative_to(self.root)
            except ValueError:
                self.error(path, 1, "Markdown source leaves the repository")
                return self.documents[path]
            lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
        except (OSError, UnicodeError, RuntimeError) as exc:
            self.error(path, 1, "cannot read Markdown: {}".format(exc))
            return self.documents[path]
        fence, body, previous = None, [], ""
        for number, original in enumerate(lines, 1):
            line = original.rstrip("\r\n")
            if fence:
                marker, language, opening, indent = fence
                if re.fullmatch(r" {0,3}" + re.escape(marker[0]) +
                                "{" + str(len(marker)) + r",}[ \t]*", line):
                    if language in ("sh", "bash", "shell"):
                        blocks.append((opening, "".join(body)))
                    fence, body = None, []
                else:
                    remove = min(indent, len(original) - len(original.lstrip(" ")))
                    body.append(original[remove:])
                continue
            match = FENCE.match(line)
            if match:
                marker, info = match.groups()
                if marker[0] == "`" and "`" in info:
                    self.error(path, number, "backtick fence info cannot contain backticks")
                language = info.strip().split()[0] if info.strip() else ""
                fence = marker, language, number, len(line) - len(line.lstrip(" "))
                previous = ""
                continue
            if re.match(r"^\s*(?:(?:>\s*)+|(?:[-+*]|\d+[.)])\s+)?(?:`{3,}|~{3,})", line):
                self.error(path, number, "unsupported container/indented fence; use a root-level fence")
            visible.append((number, line))
            match = HEADING.match(line)
            if match:
                heading = re.sub(r"(?:^|[ \t]+)#+[ \t]*$", "", match.group(1) or "").strip()
                plain = code_spans(heading)
                if re.search(r"[\[\]<>*]|(?:^|\s)_[^ ]", plain):
                    self.error(path, number, "unsupported heading markup; use plain text and inline code")
                heading = code_spans(heading, keep_text=True).lower()
                base = "".join(c for c in heading if c in "-_ " or
                               not unicodedata.category(c).startswith(("P", "S", "C"))).replace(" ", "-")
                slug, suffix = base, 0
                while slug in anchors:
                    suffix += 1
                    slug = "{}-{}".format(base, suffix)
                anchors.add(slug)
            elif previous.strip() and re.fullmatch(r" {0,3}(?:=+|-+)[ \t]*", line):
                self.error(path, number, "unsupported setext heading; use an ATX # heading")
            previous = line
        if fence:
            self.error(path, fence[2], "unclosed {} fence".format(fence[0]))
        return self.documents[path]

    def inline_links(self, path, number, original):
        line = code_spans(original)
        if re.search(r"<(?:a|img)\b", line, re.I):
            self.error(path, number, "unsupported HTML link/image; use an inline Markdown link")
        if re.match(r"^ {0,3}\[[^]]+\]:", line):
            self.error(path, number, "unsupported reference definition; use inline links")
            return
        i = 0
        while i < len(line):
            if line[i] == "\\":
                i += 2
                continue
            if line[i] != "[":
                i += 1
                continue
            end, depth = i + 1, 1
            while end < len(line) and depth:
                if line[end] == "\\":
                    end += 2
                    continue
                depth += (line[end] == "[") - (line[end] == "]")
                end += 1
            if depth:
                break
            # A linked image has its own local destination inside the outer label.
            if "[" in line[i + 1:end - 1]:
                yield from self.inline_links(path, number, line[i + 1:end - 1])
            if line[end:end + 1] == "[":
                self.error(path, number, "unsupported reference link; use an inline link")
            if line[end:end + 1] != "(":
                i = end
                continue
            try:
                target, i = destination(line, end + 1)
            except ValueError as exc:
                self.error(path, number, str(exc))
                break
            yield decode_text(target)

    def check_link(self, path, number, target):
        if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", target) or target.startswith("//"):
            return
        self.links += 1
        try:
            # urlsplit silently strips tabs/newlines and leading C0 controls.
            # Reject these paths instead of checking a different destination.
            if any(unicodedata.category(char) == "Cc" for char in target):
                raise ValueError("control characters in local URLs are unsupported")
            url = urlsplit(target)
            if url.query or "?" in target.partition("#")[0]:
                raise ValueError("local URL queries are unsupported")
            if re.search(r"%(?![0-9a-fA-F]{2})", target):
                raise ValueError("invalid percent escape")
            name, fragment = unquote(url.path, errors="strict"), unquote(url.fragment, errors="strict")
            if any(unicodedata.category(char) == "Cc" for char in name + fragment):
                raise ValueError("control characters in local URLs are unsupported")
            if name.startswith("/"):
                raise ValueError("absolute local paths are unsupported; use relative paths")
            destination_path = (path.parent / name).resolve() if name else path
            try:
                destination_path.relative_to(self.root)
            except ValueError:
                raise ValueError("local destination leaves the repository")
            if not destination_path.exists():
                raise ValueError("missing local destination")
            if fragment:
                if not destination_path.is_file() or destination_path.suffix.lower() not in (".md", ".markdown"):
                    raise ValueError("fragments require a Markdown file with ATX headings")
                if fragment not in self.document(destination_path)[2]:
                    raise ValueError("missing heading fragment #{}".format(fragment))
        except (ValueError, OSError, RuntimeError) as exc:
            self.error(path, number, "{}: {!r}".format(exc, target))

    def check_shell(self, path, opening, body):
        self.shell_blocks += 1
        try:
            # A fresh environment excludes BASH_ENV, exported functions and shell
            # options. Only the parser reads stdin; document commands never run.
            result = subprocess.run(
                ["bash", "--noprofile", "--norc", "-n"], input=body, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env={"PATH": os.defpath, "LC_ALL": "C"}, timeout=10)
        except (OSError, subprocess.TimeoutExpired) as exc:
            self.error(path, opening, "cannot check shell syntax: {}".format(exc))
            return
        if result.returncode or result.stderr:
            detail = result.stderr.strip().replace("\n", " | ")
            match = re.search(r"line (\d+)", detail)
            number = opening + int(match.group(1)) if match else opening
            self.error(path, number, "shell syntax (fence at line {}): {}".format(opening, detail))

    def run(self):
        paths = [self.root / "README.md"] + sorted((self.root / "docs").rglob("*.md"))
        for path in paths:
            visible, blocks, _ = self.document(path)
            for number, line in visible:
                for target in self.inline_links(path, number, line):
                    self.check_link(path, number, target)
            for opening, body in blocks:
                self.check_shell(path, opening, body)
        if self.errors:
            for error in self.errors:
                print(error, file=sys.stderr)
            print("{} documentation check(s) failed".format(len(self.errors)), file=sys.stderr)
            return 1
        print("Checked {} documents, {} local links/images, {} shell blocks (syntax only).".format(
            len(paths), self.links, self.shell_blocks))
        return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1],
                        help="repository root (default: parent of this tool's directory)")
    args = parser.parse_args()
    return Checker(args.root).run()


if __name__ == "__main__":
    sys.exit(main())
