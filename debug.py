import json, re, collections
import server as s

L = s.load_ledger()
rows = L["rows"]
print("source:", L["source"], "| rows after dedupe:", len(rows))
print("keys:", sorted({k for r in rows for k in r}))
print("sample row:", json.dumps(rows[0], indent=2, default=str))
print("rates:", {k: str(v) for k, v in L["rates"].items()})
print("statuses:", collections.Counter(s.row_status(r) for r in rows))
print("currencies:", collections.Counter(str(s.get_value(r, ["currency"], "USD")) for r in rows))
print("regions:", collections.Counter(s.row_region(r) for r in rows))
print("date formats:", collections.Counter(
    re.sub(r"\d", "9", str(s.get_value(r, s.DATE_KEYS))) for r in rows).most_common(5))
print("updated formats:", collections.Counter(
    re.sub(r"\d", "9", str(s.get_value(r, s.UPDATED_KEYS))) for r in rows).most_common(5))
for q in ["How much did South earn in June 2026, in US dollars?",
          "What was the total revenue in USD from the North region in March 2026?"]:
    print(q, "->", s.answer_question(q))
