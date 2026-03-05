from pydantic import BaseModel


class ParsedTask(BaseModel):
    description: str
    due_date: str                        # YYYY-MM-DD
    due_time: str | None = None          # HH:MM or None
    confidence: float = 1.0              # 0.0–1.0
    recurrence_rule: str | None = None   # e.g. "weekly:saturday", "daily"
    label_names: list[str] = []          # e.g. ["Home", "Errands"]
    notes: str | None = None             # extra context/details
