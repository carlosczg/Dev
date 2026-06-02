# Dev

Espacio de trabajo de Dev.

## Herramientas disponibles

- `sql_complexity.py`: analiza un script SQL y calcula métricas de complejidad.
- `sql_to_pyspark.py`: recibe un archivo SQL y genera un archivo Python con código PySpark en la misma ruta relativa del SQL, usando el sufijo `_pyspark.py` por defecto.
- `GUIA_CODEX_GITHUB_VSCODE_ZAT.md`: paso a paso para usar Codex, GitHub y VS Code desde cero en ZAT.

### Convertir SQL a PySpark

```bash
python3 sql_to_pyspark.py ruta/consulta.sql
```

Salida esperada por defecto:

```text
ruta/consulta_pyspark.py
```

También puedes indicar una ruta de salida manual:

```bash
python3 sql_to_pyspark.py ruta/consulta.sql --output ruta/consulta_convertida.py
```
