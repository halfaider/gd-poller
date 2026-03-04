import re
import sys

from PTN.parse import PTN
from PTN.patterns import patterns, delimiters
from PTN.extras import link_patterns

patterns["season"].append(
    r"\b(?:Complete"
    + delimiters
    + ")?s([0-9]{1,3})"
    + link_patterns(patterns["episode"])
    + r"?\b"
)
patterns["season"].append(
    r"\b(?:Complete" + delimiters + r")?Season[\. -]([0-9]{1,3})\b"
)
patterns["season"].append(r"\b([0-9]{1,3})x[0-9]{2}\b")


class FilenameParser(PTN):

    KOR_PATTERNS = {
        "title": (
            re.compile(
                r"^(?P<title>.+?)(?=\.(?:\d{4}|S\d+|E\d+|\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])))",
                re.IGNORECASE,
            ),
        ),
        "date": (
            re.compile(r"(?P<date>\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01]))"),
        ),
    }

    def process_title(self) -> None:
        super().process_title()

        title_val = self.parts.get("title")
        title_span = self.part_slices.get("title")

        if not title_val or not title_span:
            return

        anchor_match = next(
            (
                match
                for ptn in self.KOR_PATTERNS["title"]
                if (match := ptn.search(self.torrent_name))
            ),
            None,
        )

        if anchor_match:
            anchor_start, anchor_end = anchor_match.span("title")
            if anchor_end < title_span[1]:
                if title_span in self.match_slices:
                    self.match_slices.remove(title_span)

                raw_anchor_title = anchor_match.group("title")
                cleaned_anchor = " ".join(
                    raw_anchor_title.replace(".", " ").split()
                ).strip()
                self._part(
                    "title", (anchor_start, anchor_end), cleaned_anchor, overwrite=True
                )
                title_span = (anchor_start, anchor_end)

        date_match = next(
            (
                match
                for ptn in self.KOR_PATTERNS["date"]
                if (match := ptn.search(self.torrent_name))
            ),
            None,
        )
        if date_match:
            date_start, date_end = date_match.span("date")
            is_already_matched = any(
                # 다른 파트에서 사용중인 영역인지 확인
                (date_start < existing[1] and date_end > existing[0])
                for name, existing in self.part_slices.items()
                if name not in ("title", "excess", "year", "month", "day")
            )
            if not is_already_matched:
                date_text = date_match.group("date")
                year, month, day = date_text[:2], date_text[2:4], date_text[4:]
                self._part(
                    "year", (date_start, date_start + 2), "20" + year, overwrite=True
                )
                self._part(
                    "month", (date_start + 2, date_start + 4), month, overwrite=True
                )
                self._part("day", (date_start + 4, date_start + 6), day, overwrite=True)

                # 날짜가 기존 title 영역 안에 있었다면 title을 영역을 짧게 갱신
                if date_start < title_span[1]:
                    if title_span in self.match_slices:
                        self.match_slices.remove(title_span)
                    new_title = self.torrent_name[title_span[0] : date_start].strip(
                        " ."
                    )
                    self._part(
                        "title", (title_span[0], date_start), new_title, overwrite=True
                    )

            self.merge_match_slices()


def filename_parse(
    filename: str, standardise: bool = True, coherent_types: bool = False
) -> dict:
    return FilenameParser().parse(filename, standardise, coherent_types)


if __name__ == "__main__":
    if len(sys.argv) > 0:
        print(filename_parse(sys.argv[1]))
