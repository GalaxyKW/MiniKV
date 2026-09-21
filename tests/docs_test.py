#!/usr/bin/env python3
"""Exercise the documentation CLI on isolated fixtures; no services or builds."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


CHECKER = Path(__file__).resolve().parents[1] / "tools" / "check_docs.py"


class DocumentationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="minikv-docs-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / "repo"
        self.root.mkdir()
        self.write("README.md", "# Root\n")

    def write(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def check(self, env=None, cwd=None, script=CHECKER, explicit_root=True):
        command = [sys.executable, str(script)]
        if explicit_root:
            command += ["--root", str(self.root)]
        return subprocess.run(command, cwd=cwd or self.base, env=env, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)

    def assert_passes(self, result):
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertIn("syntax only", result.stdout)

    def test_relative_encoded_source_directory_image_and_nested_docs(self):
        self.write("src/main.cpp", "// source\n")
        self.write("src/thing(copy).cpp", "// source\n")
        self.write("assets/图 (copy).png", "fixture")
        self.write("docs/nested/使用 指南.md", "# 使用 `value`\n[返回](../../README.md#root)\n")
        self.write("README.md", """# Root
[directory](src/) [source](src/main.cpp)
[parentheses](src/thing(copy).cpp) [escaped](src/thing\\(copy\\).cpp)
[guide](docs/nested/%E4%BD%BF%E7%94%A8%20%E6%8C%87%E5%8D%97.md#使用-value)
![plot](<assets/图 (copy).png> "title")
[![plot](assets/%E5%9B%BE%20%28copy%29.png)](docs/nested/使用%20指南.md)
[external](https://example.invalid/missing#heading) [email](mailto:user@example.invalid)
[protocol relative](//example.invalid/file)
""")
        self.assert_passes(self.check())
        self.write("docs/nested/使用 指南.md", "# 使用 `value`\n[bad](missing.md)\n")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("docs/nested/使用 指南.md:2: missing local destination", result.stderr)

    def test_duplicate_heading_slugs_reserve_existing_suffixes(self):
        self.write("README.md", """# Root
## 重复 `value`
## 重复 `value`
## 重复 value-1
## 重复 `value`
## Topic-1
## Topic
## Topic
[one](#重复-value) [two](#重复-value-1) [literal](#重复-value-1-1)
[three](#%E9%87%8D%E5%A4%8D-value-2) [collision](#topic-2)
""")
        self.assert_passes(self.check())
        with (self.root / "README.md").open("a", encoding="utf-8") as stream:
            stream.write("[absent](#重复-value-3)\n")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing heading fragment #重复-value-3", result.stderr)

    def test_missing_links_images_and_fragments_are_all_reported(self):
        self.write("README.md", """# Root
[bad file](missing.md)
![bad image](missing.png)
[bad fragment](#absent)
[![nested image](nested.png)](#root)
""")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        for number in (2, 3, 4, 5):
            self.assertIn("README.md:{}:".format(number), result.stderr)
        self.assertIn("4 documentation check(s) failed", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_inline_code_heading_entities_are_literal_and_spaces_are_normalized(self):
        cases = (
            ("Code `&amp;`", "code-amp", "code-"),
            ("Code ` x `", "code-x", "code--x-"),
            ("Code `  x  `", "code--x-", "code-x"),
            ("Code `   `", "code----", "code--"),
            ("Plain &#65; `&amp;`", "plain-a-amp", "plain-65-amp"),
            ("Code `\\&#65;`", "code-65", "code-a"),
        )
        for heading, correct, incorrect in cases:
            with self.subTest(heading=heading):
                self.write("README.md", "## {}\n[correct](#{})\n".format(heading, correct))
                self.assert_passes(self.check())
                self.write("README.md", "## {}\n[incorrect](#{})\n".format(heading, incorrect))
                result = self.check()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("missing heading fragment #{}".format(incorrect), result.stderr)

    def test_link_entities_do_not_retarget_literal_or_escaped_ampersands(self):
        cases = (
            ("copy&copy.png", "copy&copy.png", "copy©.png"),
            (r"copy\&amp;.png", "copy&amp;.png", "copy&.png"),
            ("copy&notit;.png", "copy&notit;.png", "copy¬it;.png"),
            ("copy&amp;amp;.png", "copy&amp;.png", "copy&.png"),
            ("copy&amp;.png", "copy&.png", "copy&amp;.png"),
            ("copy&#xFFFF;.png", "copy\uffff.png", "copy.png"),
            ("copy&#0;.png", "copy\ufffd.png", "copy.png"),
        )
        for number, (raw, actual, wrong) in enumerate(cases):
            with self.subTest(destination=raw):
                directory = "assets/{}".format(number)
                self.write("README.md", "# Root\n[link]({}/{})\n".format(directory, raw))
                self.write("{}/{}".format(directory, wrong), "wrong destination")
                result = self.check()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("README.md:2: missing local destination", result.stderr)
                self.write("{}/{}".format(directory, actual), "actual destination")
                self.assert_passes(self.check())

    def test_control_references_cannot_be_silently_removed_from_links(self):
        self.write("copy.png", "would match after stripping the control")
        self.write("copy€.png", "would match HTML's legacy numeric remapping")
        for encoded in ("&#1;", "&#9;", "&#10;", "&#x80;", "%09", "%0A"):
            with self.subTest(encoded=encoded):
                self.write("README.md", "# Root\n[link](copy{}.png)\n".format(encoded))
                result = self.check()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("control characters in local URLs are unsupported", result.stderr)

    def test_heading_entities_follow_markdown_escape_and_semicolon_rules(self):
        cases = (
            ("Copy &copy", "copy-copy", "copy-"),
            (r"Copy \&amp;", "copy-amp", "copy-"),
            ("Copy &notit;", "copy-notit", "copy-it"),
            ("Copy &#00000065;", "copy-00000065", "copy-a"),
            ("Copy &#x0000041;", "copy-x0000041", "copy-a"),
            ("Copy &#65;", "copy-a", "copy-65"),
            ("Copy &#x41;", "copy-a", "copy-x41"),
        )
        for heading, correct, incorrect in cases:
            with self.subTest(heading=heading):
                self.write("README.md", "## {}\n[correct](#{})\n".format(heading, correct))
                self.assert_passes(self.check())
                self.write("README.md", "## {}\n[incorrect](#{})\n".format(heading, incorrect))
                result = self.check()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("missing heading fragment #{}".format(incorrect), result.stderr)

    def test_code_fences_and_inline_code_hide_fake_links_and_headings(self):
        self.write("README.md", """# Root
`[literal](missing.md)` and ``[literal](other.md)``
```text
# Phantom
[missing](absent.md)
~~~sh
if then
~~~
```
````bash
cat <<'EOF'
```sh
# Shell heading
[missing](not-here)
EOF
`````
~~~shell
printf '%s\\n' ok
~~~~
[root](#root)
""")
        result = self.check()
        self.assert_passes(result)
        self.assertIn("2 shell blocks", result.stdout)
        with (self.root / "README.md").open("a", encoding="utf-8") as stream:
            stream.write("[fake](#phantom)\n[fake](#shell-heading)\n")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing heading fragment #phantom", result.stderr)
        self.assertIn("missing heading fragment #shell-heading", result.stderr)

    def test_unclosed_or_wrongly_closed_fences_fail(self):
        for content in ("```sh\necho ok\n", "~~~text\ntext\n```\n",
                        "````bash\necho ok\n```\n"):
            with self.subTest(content=content):
                self.write("README.md", "# Root\n" + content)
                result = self.check()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("README.md:2: unclosed", result.stderr)

    def test_escaped_backticks_do_not_hide_real_links(self):
        self.write("README.md", "# Root\n\\`[broken](missing.md)\\`\n")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("README.md:2: missing local destination", result.stderr)
        # Inside an actual code span, a backslash does not escape its closing tick.
        self.write("README.md", "# Root\n`literal\\` [broken](missing.md)\n")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("README.md:2: missing local destination", result.stderr)

    def test_shell_syntax_errors_include_source_location(self):
        self.write("README.md", "# Root\n\n~~~bash\nif then\n~~~\n")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("README.md:4: shell syntax (fence at line 3)", result.stderr)

    def test_shell_content_and_startup_hooks_are_never_executed(self):
        sentinel = self.base / "executed"
        startup_sentinel = self.base / "startup-executed"
        startup = self.base / "startup.sh"
        startup.write_text("touch '{}'\n".format(startup_sentinel), encoding="utf-8")
        self.write("README.md", """# Root
```sh
touch '{}'
echo "$(touch '{}')"
exit 17
```
""".format(sentinel, sentinel))
        environment = os.environ.copy()
        environment["BASH_ENV"] = str(startup)
        environment["ENV"] = str(startup)
        self.assert_passes(self.check(env=environment))
        self.assertFalse(sentinel.exists(), "document commands must never execute")
        self.assertFalse(startup_sentinel.exists(), "shell startup hooks must never execute")

    def test_indented_root_fence_preserves_heredoc_syntax(self):
        self.write("README.md", "# Root\n   ```sh\n   cat <<'EOF'\n   body\n   EOF\n   ```\n")
        self.assert_passes(self.check())

    def test_script_default_root_is_independent_of_callers_directory(self):
        copied = self.root / "tools" / "check_docs.py"
        copied.parent.mkdir()
        shutil.copyfile(CHECKER, copied)
        elsewhere = self.base / "elsewhere"
        elsewhere.mkdir()
        (elsewhere / "README.md").write_text("[bad](missing)\n", encoding="utf-8")
        self.assert_passes(self.check(cwd=elsewhere, script=copied, explicit_root=False))
        self.write("README.md", "[bad](missing)\n")
        result = self.check(cwd=elsewhere, script=copied, explicit_root=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("README.md:1: missing local destination", result.stderr)

    def test_missing_readme_and_invalid_utf8_fail_cleanly(self):
        (self.root / "README.md").unlink()
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("README.md:1: cannot read Markdown", result.stderr)
        self.write("README.md", "# Root\n")
        bad = self.write("docs/bad.md", "")
        bad.write_bytes(b"\xff\n")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("docs/bad.md:1: cannot read Markdown", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_unsupported_syntax_is_rejected_explicitly(self):
        cases = (
            ("[link][ref]\n[ref]: README.md\n", "unsupported reference"),
            ('<a href="README.md">link</a>\n', "unsupported HTML link/image"),
            ('<img src="missing.png">\n', "unsupported HTML link/image"),
            ("Title\n=====\n", "unsupported setext heading"),
            ("> ```sh\n> echo ok\n> ```\n", "unsupported container/indented fence"),
            ("## [linked](README.md)\n", "unsupported heading markup"),
            ("[link](README.md?raw=1)\n", "local URL queries are unsupported"),
            ("[link](README.md#%FF)\n", "invalid start byte"),
            ("[link](README.md#%xx)\n", "invalid percent escape"),
            ("[link](README.md\n", "unsupported or unclosed inline link"),
        )
        for content, diagnostic in cases:
            with self.subTest(content=content):
                self.write("README.md", content)
                result = self.check()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(diagnostic, result.stderr)

    def test_non_markdown_fragments_and_destinations_outside_repo_fail(self):
        self.write("src/main.cpp", "// source\n")
        outside = self.base / "outside.md"
        outside.write_text("# Outside\n", encoding="utf-8")
        (self.root / "escaped.md").symlink_to(outside)
        for target, diagnostic in (
            ("src/main.cpp#L1", "fragments require a Markdown file"),
            ("src/#heading", "fragments require a Markdown file"),
            ("../outside.md", "leaves the repository"),
            ("escaped.md", "leaves the repository"),
            (str(outside), "absolute local paths are unsupported"),
        ):
            with self.subTest(target=target):
                self.write("README.md", "[link]({})\n".format(target))
                result = self.check()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(diagnostic, result.stderr)

    def test_markdown_sources_cannot_be_symlinks_outside_repo(self):
        outside = self.base / "outside.md"
        outside.write_text("# Outside\n", encoding="utf-8")
        readme = self.root / "README.md"
        readme.unlink()
        readme.symlink_to(outside)
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("README.md:1: Markdown source leaves the repository", result.stderr)
        readme.unlink()
        self.write("README.md", "# Root\n")
        docs = self.root / "docs"
        docs.mkdir()
        (docs / "outside.md").symlink_to(outside)
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("docs/outside.md:1: Markdown source leaves the repository", result.stderr)


if __name__ == "__main__":
    unittest.main()
