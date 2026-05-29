"""Generador de código PySpark a partir de un archivo SQL.

El script recibe como entrada la ruta de un archivo ``.sql`` y crea, por
 defecto, un archivo Python al lado del SQL original con sufijo
``_pyspark.py``. La salida generada puede ejecutar las sentencias SQL con
``spark.sql`` y, para consultas ``SELECT`` simples, también incluye una
traducción aproximada a la API de DataFrames de PySpark.

Ejemplo:
    python3 sql_to_pyspark.py consultas/ventas.sql

Salida por defecto:
    consultas/ventas_pyspark.py
"""
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional


@dataclass
class SelectParts:
    """Partes reconocidas de un SELECT simple."""

    columns: str
    table: str
    where: Optional[str] = None
    group_by: Optional[str] = None
    order_by: Optional[str] = None
    limit: Optional[str] = None


def remove_sql_comments(sql: str) -> str:
    """Elimina comentarios SQL de línea y de bloque."""
    without_block_comments = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    return re.sub(r"--.*?$", "", without_block_comments, flags=re.M)


def split_sql_statements(sql: str) -> List[str]:
    """Separa sentencias por ``;`` sin partir textos entre comillas."""
    statements: List[str] = []
    current: List[str] = []
    in_single_quote = False
    in_double_quote = False
    index = 0

    while index < len(sql):
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < len(sql) else ""

        if char == "'" and not in_double_quote:
            current.append(char)
            if in_single_quote and next_char == "'":
                current.append(next_char)
                index += 2
                continue
            in_single_quote = not in_single_quote
            index += 1
            continue

        if char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(char)
            index += 1
            continue

        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    final_statement = "".join(current).strip()
    if final_statement:
        statements.append(final_statement)

    return statements


def normalize_spaces(text: str) -> str:
    """Convierte saltos de línea y espacios repetidos en un solo espacio."""
    return re.sub(r"\s+", " ", text).strip()


def split_csv_expressions(text: str) -> List[str]:
    """Separa expresiones por coma respetando paréntesis y comillas."""
    expressions: List[str] = []
    current: List[str] = []
    depth = 0
    in_single_quote = False
    in_double_quote = False
    index = 0

    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""

        if char == "'" and not in_double_quote:
            current.append(char)
            if in_single_quote and next_char == "'":
                current.append(next_char)
                index += 2
                continue
            in_single_quote = not in_single_quote
            index += 1
            continue

        if char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(char)
            index += 1
            continue

        if not in_single_quote and not in_double_quote:
            if char == "(":
                depth += 1
            elif char == ")" and depth > 0:
                depth -= 1
            elif char == "," and depth == 0:
                expression = "".join(current).strip()
                if expression:
                    expressions.append(expression)
                current = []
                index += 1
                continue

        current.append(char)
        index += 1

    expression = "".join(current).strip()
    if expression:
        expressions.append(expression)

    return expressions


def parse_simple_select(statement: str) -> Optional[SelectParts]:
    """Reconoce SELECTs simples de una sola tabla y sin JOIN/subconsultas."""
    compact = normalize_spaces(statement).rstrip(";")
    lowered = compact.lower()

    if not lowered.startswith("select "):
        return None
    if re.search(r"\b(join|union|intersect|except|with)\b", compact, flags=re.I):
        return None
    if re.search(r"\bfrom\s*\(", compact, flags=re.I):
        return None

    match = re.match(
        r"^select\s+(?P<columns>.+?)\s+from\s+(?P<table>[`\w.]+)(?P<rest>.*)$",
        compact,
        flags=re.I,
    )
    if not match:
        return None

    columns = match.group("columns").strip()
    table = match.group("table").strip("`")
    rest = match.group("rest").strip()

    clauses = {"where": None, "group_by": None, "order_by": None, "limit": None}
    clause_pattern = re.compile(
        r"\b(where|group\s+by|order\s+by|limit)\b",
        flags=re.I,
    )
    matches = list(clause_pattern.finditer(rest))

    for position, clause_match in enumerate(matches):
        clause_name = clause_match.group(1).lower().replace(" ", "_")
        start = clause_match.end()
        end = matches[position + 1].start() if position + 1 < len(matches) else len(rest)
        clauses[clause_name] = rest[start:end].strip()

    return SelectParts(
        columns=columns,
        table=table,
        where=clauses["where"],
        group_by=clauses["group_by"],
        order_by=clauses["order_by"],
        limit=clauses["limit"],
    )


def py_string_literal(value: str) -> str:
    """Representa un texto como literal Python seguro."""
    return repr(value)


def triple_quoted_sql(statement: str) -> str:
    """Escapa una sentencia para incluirla en un string triple-comilla."""
    return statement.replace('"""', '\\\"\\\"\\\"')


def dataframe_translation(parts: SelectParts, result_name: str) -> List[str]:
    """Genera líneas PySpark DataFrame API para un SELECT simple."""
    lines = [f'{result_name} = spark.table({py_string_literal(parts.table)})']

    if parts.where:
        lines.append(f"{result_name} = {result_name}.where({py_string_literal(parts.where)})")

    if parts.group_by:
        group_columns = split_csv_expressions(parts.group_by)
        group_args = ", ".join(py_string_literal(column) for column in group_columns)
        select_columns = split_csv_expressions(parts.columns)
        lines.append(f"{result_name} = {result_name}.groupBy({group_args})")
        lines.append(
            f"# TODO: revisar agregaciones del SELECT original: {py_string_literal(', '.join(select_columns))}"
        )
        lines.append(f"# {result_name} = {result_name}.agg(...)")
    elif parts.columns != "*":
        select_columns = split_csv_expressions(parts.columns)
        select_args = ", ".join(py_string_literal(column) for column in select_columns)
        lines.append(f"{result_name} = {result_name}.selectExpr({select_args})")

    if parts.order_by:
        order_columns = split_csv_expressions(parts.order_by)
        order_args = ", ".join(py_string_literal(column) for column in order_columns)
        lines.append(f"{result_name} = {result_name}.orderBy({order_args})")

    if parts.limit:
        limit_value = parts.limit.split()[0]
        if limit_value.isdigit():
            lines.append(f"{result_name} = {result_name}.limit({limit_value})")
        else:
            lines.append(f"# TODO: revisar LIMIT no numérico del SQL original: {py_string_literal(parts.limit)}")

    return lines


def statement_variable_name(index: int, statement: str) -> str:
    """Define un nombre legible para el resultado de una sentencia."""
    first_word_match = re.match(r"\s*(\w+)", statement)
    first_word = first_word_match.group(1).lower() if first_word_match else "statement"
    return f"{first_word}_{index}"


def render_statement(index: int, statement: str) -> str:
    """Genera el bloque Python para una sentencia SQL."""
    statement_name = statement_variable_name(index, statement)
    query_name = f"query_{index}"
    cleaned_statement = statement.strip().rstrip(";")
    simple_select = parse_simple_select(cleaned_statement)

    lines = [
        "",
        f"# Sentencia {index}",
        f'{query_name} = """\\',
        triple_quoted_sql(cleaned_statement),
        '"""',
    ]

    if simple_select and not simple_select.group_by:
        lines.extend(
            [
                "# Traducción aproximada a DataFrame API para SELECT simple.",
                "# Si necesitas máxima fidelidad SQL, usa la alternativa spark.sql de abajo.",
                *dataframe_translation(simple_select, statement_name),
                f"{statement_name}.show(truncate=False)",
                "",
                "# Alternativa fiel al SQL original:",
                f"# {statement_name} = spark.sql({query_name})",
            ]
        )
    elif simple_select and simple_select.group_by:
        skeleton = dataframe_translation(simple_select, statement_name)
        commented_skeleton = [f"# {line}" for line in skeleton]
        lines.extend(
            [
                "# SELECT con GROUP BY detectado.",
                "# Se ejecuta con spark.sql para preservar agregaciones y semántica del SQL original.",
                f"{statement_name} = spark.sql({query_name})",
                f"{statement_name}.show(truncate=False)",
                "",
                "# Boceto opcional para convertir manualmente a DataFrame API:",
                *commented_skeleton,
            ]
        )
    else:
        lines.extend(
            [
                "# Sentencia compleja o de escritura: se conserva con spark.sql para mantener fidelidad.",
                f"{statement_name} = spark.sql({query_name})",
                f"if {statement_name} is not None:",
                f"    {statement_name}.show(truncate=False)",
            ]
        )

    return "\n".join(lines)


def render_pyspark_script(sql_path: Path, statements: Iterable[str]) -> str:
    """Renderiza el contenido completo del archivo Python de salida."""
    statement_blocks = [render_statement(index, statement) for index, statement in enumerate(statements, start=1)]
    source_name = sql_path.as_posix()

    return "\n".join(
        [
            '"""Código PySpark generado desde un archivo SQL.',
            "",
            f"Archivo SQL de origen: {source_name}",
            "",
            "Nota: las traducciones a DataFrame API son aproximadas para SELECTs simples.",
            "Para SQL complejo, el generador conserva la sentencia original usando spark.sql.",
            '"""',
            "from pyspark.sql import SparkSession",
            "",
            "",
            "spark = (",
            "    SparkSession.builder",
            f"    .appName({py_string_literal('SQL to PySpark - ' + sql_path.stem)})",
            "    .getOrCreate()",
            ")",
            *statement_blocks,
            "",
        ]
    )


def default_output_path(sql_path: Path) -> Path:
    """Crea la ruta relativa de salida al lado del archivo SQL original."""
    return sql_path.with_name(f"{sql_path.stem}_pyspark.py")


def convert_sql_file(sql_path: Path, output_path: Optional[Path] = None) -> Path:
    """Lee un archivo SQL y escribe su traducción PySpark."""
    if not sql_path.exists():
        raise FileNotFoundError(f"No existe el archivo SQL: {sql_path}")
    if not sql_path.is_file():
        raise ValueError(f"La ruta no es un archivo: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    statements = split_sql_statements(remove_sql_comments(sql_text))
    if not statements:
        raise ValueError(f"No se encontraron sentencias SQL en: {sql_path}")

    destination = output_path or default_output_path(sql_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_pyspark_script(sql_path, statements), encoding="utf-8")
    return destination


def parse_args() -> argparse.Namespace:
    """Procesa argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Genera un archivo Python/PySpark a partir de un archivo SQL."
    )
    parser.add_argument(
        "sql_file",
        help="Nombre o ruta relativa del archivo SQL de entrada.",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Ruta opcional del archivo Python de salida. Por defecto se crea al lado del SQL con sufijo _pyspark.py.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sql_path = Path(args.sql_file)
    output_path = Path(args.output) if args.output else None
    generated_path = convert_sql_file(sql_path, output_path)
    print(f"Archivo PySpark generado: {generated_path}")


if __name__ == "__main__":
    main()
