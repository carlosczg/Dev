import re

def analyze_sql_complexity(file_path):
    # Define regex patterns
    select_pattern = re.compile(r"\bSELECT\b", re.IGNORECASE)
    update_pattern = re.compile(r"\bUPDATE\b", re.IGNORECASE)
    delete_pattern = re.compile(r"\bDELETE\b", re.IGNORECASE)
    merge_pattern = re.compile(r"\bMERGE\b", re.IGNORECASE)
    analytic_function_pattern = re.compile(r"\b(RANK|DENSE_RANK|ROW_NUMBER|NTILE|CUME_DIST|PERCENT_RANK)\b\(", re.IGNORECASE)
    
    # Initialize counters
    total_sql_steps = 0
    select_count = 0
    update_count = 0
    delete_count = 0
    merge_count = 0
    analytic_function_count = 0
    keywords_count = 0  # To count all SQL-related keywords as an additional metric

    # Read the SQL file
    with open(file_path, 'r', encoding='utf-8') as file:
        sql_content = file.read()

    # Count occurrences
    select_count = len(select_pattern.findall(sql_content))
    update_count = len(update_pattern.findall(sql_content))
    delete_count = len(delete_pattern.findall(sql_content))
    merge_count = len(merge_pattern.findall(sql_content))
    analytic_function_count = len(analytic_function_pattern.findall(sql_content))

    # Calculate total SQL steps
    total_sql_steps = select_count + update_count + delete_count + merge_count

    # Additional metric: Count keywords (approx. SQL complexity density)
    keywords_pattern = re.compile(r"\b(SELECT|UPDATE|DELETE|INSERT|MERGE|JOIN|WHERE|GROUP BY|ORDER BY|HAVING|CASE|WHEN|THEN|END)\b", re.IGNORECASE)
    keywords_count = len(keywords_pattern.findall(sql_content))

    # Calculate complexity score
    complexity_score = min(10, (total_sql_steps + analytic_function_count + (keywords_count // 10)))
    complexity_explanation = (
        f"Complejidad puntuación: {complexity_score}/10. La puntuación se basa en el número de pasos SQL ({total_sql_steps}), "
        f"cantidad de funciones analíticas ({analytic_function_count}) y densidad de palabras clave ({keywords_count} instancias). "
        f"Un script con varias instrucciones y funciones analíticas tiende a ser más complejo debido a la gestión de datos "
        f"y cálculos que implica."
    )

    # Print results
    print("Análisis de Complejidad del Script SQL")
    print("--------------------------------------")
    print(f"Total de sentencias SELECT: {select_count}")
    print(f"Total de sentencias UPDATE: {update_count}")
    print(f"Total de sentencias DELETE: {delete_count}")
    print(f"Total de sentencias MERGE: {merge_count}")
    print(f"Total de funciones analíticas SQL: {analytic_function_count}")
    print(f"Pasos SQL totales: {total_sql_steps}")
    print(f"Ocurrencias de palabras clave SQL: {keywords_count}")
    print(complexity_explanation)

    return {
        "select_count": select_count,
        "update_count": update_count,
        "delete_count": delete_count,
        "merge_count": merge_count,
        "analytic_function_count": analytic_function_count,
        "total_sql_steps": total_sql_steps,
        "keywords_count": keywords_count,
        "complexity_score": complexity_score,
        "complexity_explanation": complexity_explanation
    }

# Example usage
# Replace 'example.sql' with the path to your SQL file
result = analyze_sql_complexity('example.sql')