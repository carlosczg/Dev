"""SQL complexity analyzer.

This script inspects SQL text and reports counts for several statement types
and analytic functions, plus a rough complexity score.
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable


ANALYTIC_FUNCTIONS = (
    "rank",
    "dense_rank",
    "row_number",
    "ntile",
    "cume_dist",
    "percent_rank",
)


@dataclass
class AnalysisResult:
    selects: int
    updates: int
    deletes: int
    merges: int
    sql_steps: int
    analytic_functions: int
    complexity_score: float
    score_explanation: str

    def as_dict(self) -> Dict[str, str]:
        return {
            "selects": self.selects,
            "updates": self.updates,
            "deletes": self.deletes,
            "merges": self.merges,
            "sql_steps": self.sql_steps,
            "analytic_functions": self.analytic_functions,
            "complexity_score": self.complexity_score,
            "score_explanation": self.score_explanation,
        }


def remove_comments(sql: str) -> str:
    """Remove SQL comments (both line and block)."""
    without_block = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    return re.sub(r"--.*?$", "", without_block, flags=re.M)


def count_occurrences(sql: str, keyword: str) -> int:
    pattern = rf"\b{keyword}\b"
    return len(re.findall(pattern, sql, flags=re.IGNORECASE))


def count_analytic_functions(sql: str, functions: Iterable[str]) -> int:
    return sum(
        len(re.findall(rf"\b{fn}\s*\(", sql, flags=re.IGNORECASE))
        for fn in functions
    )


def count_sql_steps(sql: str) -> int:
    statements = [segment for segment in re.split(r";", sql) if segment.strip()]
    return len(statements)


def calculate_complexity_score(counts: Dict[str, int]) -> tuple[float, str]:
    """Compute a heuristic score between 1 and 10 with an explanation."""
    weights = {
        "selects": 0.5,
        "updates": 0.8,
        "deletes": 0.8,
        "merges": 1.0,
        "analytic_functions": 1.0,
        "sql_steps": 0.3,
    }

    score = 1.0
    score += counts["selects"] * weights["selects"]
    score += counts["updates"] * weights["updates"]
    score += counts["deletes"] * weights["deletes"]
    score += counts["merges"] * weights["merges"]
    score += counts["analytic_functions"] * weights["analytic_functions"]
    score += max(counts["sql_steps"] - 1, 0) * weights["sql_steps"]
    bounded_score = min(10.0, round(score, 2))

    explanation = (
        "Base 1 punto por tener SQL; +0.5 por SELECT; +0.8 por UPDATE/DELETE; "
        "+1 por MERGE; +1 por función analítica; +0.3 por cada paso adicional. "
        "El puntaje se acota a 10."
    )
    return bounded_score, explanation


def analyze_sql(sql: str) -> AnalysisResult:
    cleaned_sql = remove_comments(sql)
    counts = {
        "selects": count_occurrences(cleaned_sql, "select"),
        "updates": count_occurrences(cleaned_sql, "update"),
        "deletes": count_occurrences(cleaned_sql, "delete"),
        "merges": count_occurrences(cleaned_sql, "merge"),
        "sql_steps": count_sql_steps(cleaned_sql),
        "analytic_functions": count_analytic_functions(cleaned_sql, ANALYTIC_FUNCTIONS),
    }
    score, explanation = calculate_complexity_score(counts)

    return AnalysisResult(
        selects=counts["selects"],
        updates=counts["updates"],
        deletes=counts["deletes"],
        merges=counts["merges"],
        sql_steps=counts["sql_steps"],
        analytic_functions=counts["analytic_functions"],
        complexity_score=score,
        score_explanation=explanation,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calcula métricas de complejidad para un script SQL."
    )
    parser.add_argument(
        "source",
        nargs="?",
        help="Ruta al archivo SQL a analizar. Si se omite, se lee de STDIN.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.source:
        sql_text = Path(args.source).read_text(encoding="utf-8")
    else:
        sql_text = sys.stdin.read()

    result = analyze_sql(sql_text)

    print("Resumen de complejidad SQL:\n")
    print(f"SELECTs totales:          {result.selects}")
    print(f"UPDATEs totales:          {result.updates}")
    print(f"DELETEs totales:          {result.deletes}")
    print(f"MERGEs totales:           {result.merges}")
    print(f"Pasos SQL (secciones ;):  {result.sql_steps}")
    print(f"Funciones analíticas:     {result.analytic_functions}")
    print()
    print(f"Puntaje de complejidad:   {result.complexity_score} / 10")
    print(f"Cálculo: {result.score_explanation}")


if __name__ == "__main__":
    main()
