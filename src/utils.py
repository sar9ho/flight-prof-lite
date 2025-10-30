from dataclasses import dataclass

@dataclass
class MonthKey:
    year: int
    month: int

def month_str(year:int, month:int) -> str:
    return f"{year:04d}-{month:02d}"
