import re
import sys

def analyze_sql_complexity(file_name):
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
    keywords_count = 0

    # Read the SQL file
    try:
        with open(file_name, 'r', encoding='utf-8') as file:
            sql_content = file.read()
    except FileNotFoundError:
        print(f"Error: El archivo '{file_name}' no existe en la ruta actual.")
        return

    # Count occurrences
    select_count = len(select_pattern.findall(sql_content))
    update_count = len(update_pattern.findall(sql_content))
    delete_count = len(delete_pattern.findall(sql_content))
    merge_count = len(merge_pattern.findall(sql_content))
    analytic_function_count = len(analytic_function_pattern.findall(sql_content))

    # Calculate total SQL steps
    total_sql_steps = select_count + update_count + delete_count + merge_count

    # Additional metric: Count keywords (complexity density)
    keywords_pattern = re.compile(r"\b(SELECT|UPDATE|DELETE|INSERT|MERGE|JOIN|WHERE|GROUP BY|ORDER BY|HAVING|CASE|WHEN|THEN|END)\b", re.IGNORECASE)
    keywords_count = len(keywords_pattern.findall(sql_content))

    # Calculate complexity score
    complexity_score = min(10, (total_sql_steps + analytic_function_count + (keywords_count // 10)))
    complexity_explanation = f"Complejidad: {complexity_score}/10"

    # Print results
    print("Análisis de Complejidad del SQL")
    print("----------------------------------")
    print(f"Total SELECTs: {select_count}")
    print(f"Total UPDATEs: {update_count}")
    print(f"Total DELETEs: {delete_count}")
    print(f"Total MERGEs: {merge_count}")
    print(f"Funciones Analíticas: {analytic_function_count}")
    print(f"Total pasos SQL: {total_sql_steps}")
    print(f"Keywords SQL: {keywords_count}")
    print(complexity_explanation)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python sql_complexity_analyzer.py <archivo.sql>")
    else:
        file_name = sys.argv[1]
        analyze_sql_complexity(file_name)