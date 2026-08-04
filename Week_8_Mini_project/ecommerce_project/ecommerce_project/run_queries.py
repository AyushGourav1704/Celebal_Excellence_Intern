"""
Executes every query in queries.sql against db/ecommerce.db and prints
the first few rows of each result set, so you can verify Part 3 works
end-to-end. Also writes full results to reports/query_outputs.txt.
"""

import re
import sqlite3

DB_PATH = "db/ecommerce.db"
SQL_PATH = "queries.sql"
OUT_PATH = "reports/query_outputs.txt"


def split_queries(sql_text):
    """Split queries.sql into (title, sql) pairs using the numbered comment headers."""
    # Match blocks starting with "-- N. Title" up to the next "-- N." or end of file
    pattern = re.compile(r"--\s*(\d+)\.\s*(.+?)\n(.*?)(?=\n--\s*\d+\.|\Z)", re.DOTALL)
    queries = []
    for match in pattern.finditer(sql_text):
        num, title, body = match.groups()
        # strip trailing comment-only lines, keep actual SQL
        sql = body.strip()
        if sql:
            queries.append((f"{num}. {title.strip()}", sql))
    return queries


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    with open(SQL_PATH) as f:
        sql_text = f.read()

    queries = split_queries(sql_text)
    out_lines = []

    for title, sql in queries:
        out_lines.append("=" * 80)
        out_lines.append(title)
        out_lines.append("=" * 80)
        try:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            out_lines.append(" | ".join(cols))
            for row in rows[:10]:
                out_lines.append(" | ".join(str(v) for v in row))
            out_lines.append(f"... ({len(rows)} total rows)")
            print(f"[OK] {title}  -> {len(rows)} rows")
        except Exception as e:
            out_lines.append(f"ERROR: {e}")
            print(f"[FAIL] {title}  -> {e}")
        out_lines.append("")

    with open(OUT_PATH, "w") as f:
        f.write("\n".join(out_lines))

    print(f"\nFull output written to {OUT_PATH}")
    conn.close()


if __name__ == "__main__":
    main()
