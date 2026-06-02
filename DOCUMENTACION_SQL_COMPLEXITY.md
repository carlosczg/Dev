# Documentación del analizador de complejidad SQL

Este documento explica cómo construimos el archivo `sql_complexity.py`, cómo conectarlo con un repositorio en GitHub, cómo usarlo y qué ideas podemos aplicar para mejorarlo en el futuro.

## 1. Objetivo del script

El objetivo de `sql_complexity.py` es analizar un script SQL y entregar un resumen fácil de interpretar con estas métricas:

- Cantidad de sentencias `SELECT`, incluyendo consultas anidadas.
- Cantidad de sentencias `UPDATE`.
- Cantidad de sentencias `DELETE`.
- Cantidad de sentencias `MERGE`.
- Cantidad de pasos SQL en general, separados por punto y coma (`;`).
- Cantidad de funciones analíticas SQL:
  - `RANK()`
  - `DENSE_RANK()`
  - `ROW_NUMBER()`
  - `NTILE()`
  - `CUME_DIST()`
  - `PERCENT_RANK()`
- Un puntaje de complejidad del 1 al 10 con una explicación del cálculo.

## 2. Paso a paso: cómo creamos el archivo `sql_complexity.py`

### Paso 1: Definimos las métricas necesarias

Primero listamos qué elementos del SQL queríamos contar. Las métricas principales fueron:

1. Sentencias SQL principales: `SELECT`, `UPDATE`, `DELETE` y `MERGE`.
2. Pasos SQL generales, usando `;` como separador de sentencias.
3. Funciones analíticas comunes.
4. Un puntaje final que ayudara a interpretar la complejidad.

### Paso 2: Creamos un script en Python

Creamos el archivo `sql_complexity.py` en la raíz del repositorio. La idea fue mantenerlo simple para que se pueda ejecutar sin instalar librerías externas.

El script usa módulos estándar de Python:

- `argparse`: para recibir una ruta de archivo por consola.
- `re`: para buscar palabras clave y funciones con expresiones regulares.
- `sys`: para leer SQL desde la entrada estándar cuando no se pasa un archivo.
- `pathlib`: para leer archivos de forma clara y moderna.
- `dataclasses`: para organizar el resultado del análisis.

### Paso 3: Limpiamos comentarios SQL

Antes de contar palabras clave, el script elimina comentarios para evitar falsos positivos. Por ejemplo, si un comentario dice `-- SELECT pendiente`, no debería contarse como una sentencia real.

El script contempla:

- Comentarios de línea con `--`.
- Comentarios de bloque con `/* ... */`.

### Paso 4: Contamos las sentencias SQL

Después de limpiar comentarios, el script busca palabras clave usando expresiones regulares con límites de palabra. Esto ayuda a contar `SELECT` como palabra completa y no como parte de otra palabra más larga.

Ejemplo conceptual:

```python
count_occurrences(cleaned_sql, "select")
```

Ese mismo patrón se aplica para `UPDATE`, `DELETE` y `MERGE`.

### Paso 5: Contamos funciones analíticas

El script revisa si aparecen funciones analíticas seguidas de paréntesis, por ejemplo:

```sql
ROW_NUMBER() OVER (PARTITION BY cliente ORDER BY fecha)
```

Actualmente cuenta estas funciones:

```text
RANK, DENSE_RANK, ROW_NUMBER, NTILE, CUME_DIST, PERCENT_RANK
```

### Paso 6: Contamos pasos SQL

Para estimar cuántos pasos tiene un script, se separa el texto por punto y coma (`;`) y se cuentan las secciones no vacías.

Ejemplo:

```sql
SELECT * FROM clientes;
UPDATE clientes SET activo = 1;
DELETE FROM clientes_temp;
```

Este ejemplo tendría 3 pasos SQL.

### Paso 7: Calculamos el puntaje de complejidad

El puntaje inicia en 1 y suma puntos según los elementos encontrados:

| Elemento | Peso |
| --- | ---: |
| Cada `SELECT` | +0.5 |
| Cada `UPDATE` | +0.8 |
| Cada `DELETE` | +0.8 |
| Cada `MERGE` | +1.0 |
| Cada función analítica | +1.0 |
| Cada paso SQL adicional | +0.3 |

El resultado se limita a un máximo de 10 para que siempre quede en una escala de 1 a 10.

> Importante: este puntaje es una heurística. Sirve como orientación rápida, no como una medición perfecta de complejidad técnica.

## 3. Cómo conectar el repositorio con GitHub

Si el repositorio todavía no está conectado a GitHub, se puede hacer de esta forma.

### Paso 1: Crear un repositorio en GitHub

1. Entrar a [GitHub](https://github.com/).
2. Hacer clic en **New repository**.
3. Escribir un nombre para el repositorio.
4. Elegir si será público o privado.
5. Crear el repositorio sin agregar archivos iniciales si ya tenemos archivos locales.

### Paso 2: Inicializar Git localmente, si todavía no existe

Desde la carpeta del proyecto:

```bash
git init
```

### Paso 3: Revisar los archivos modificados

```bash
git status
```

### Paso 4: Agregar los archivos al control de versiones

```bash
git add sql_complexity.py DOCUMENTACION_SQL_COMPLEXITY.md README.md
```

Si se quiere agregar todo lo que cambió:

```bash
git add .
```

### Paso 5: Crear un commit

```bash
git commit -m "Add SQL complexity analyzer documentation"
```

### Paso 6: Conectar el repositorio local con GitHub

Reemplaza `USUARIO` y `REPOSITORIO` por los valores reales de tu cuenta:

```bash
git remote add origin https://github.com/USUARIO/REPOSITORIO.git
```

Si el remoto ya existe y solo quieres verificarlo:

```bash
git remote -v
```

### Paso 7: Subir los cambios a GitHub

Si tu rama principal se llama `main`:

```bash
git branch -M main
git push -u origin main
```

Si estás trabajando en otra rama, por ejemplo `feature/sql-complexity-docs`:

```bash
git push -u origin feature/sql-complexity-docs
```

## 4. Cómo usar `sql_complexity.py`

### Opción A: Analizar un archivo SQL

Guarda tu SQL en un archivo, por ejemplo `consulta.sql`, y ejecuta:

```bash
python3 sql_complexity.py consulta.sql
```

### Opción B: Pegar SQL directamente por consola

También puedes pasar el SQL por entrada estándar:

```bash
python3 sql_complexity.py <<'SQL'
SELECT cliente_id, ROW_NUMBER() OVER (PARTITION BY pais ORDER BY fecha) AS rn
FROM ventas;

UPDATE ventas
SET revisado = 1;
SQL
```

### Ejemplo de salida esperada

```text
Resumen de complejidad SQL:

SELECTs totales:          1
UPDATEs totales:          1
DELETEs totales:          0
MERGEs totales:           0
Pasos SQL (secciones ;):  2
Funciones analíticas:     1

Puntaje de complejidad:   3.6 / 10
Cálculo: Base 1 punto por tener SQL; +0.5 por SELECT; +0.8 por UPDATE/DELETE; +1 por MERGE; +1 por función analítica; +0.3 por cada paso adicional. El puntaje se acota a 10.
```

## 5. Cómo interpretar el resultado

Una lectura sugerida del puntaje es:

| Puntaje | Interpretación |
| ---: | --- |
| 1 a 3 | SQL simple: pocos pasos y baja transformación. |
| 4 a 6 | SQL medio: varias operaciones o funciones analíticas. |
| 7 a 8 | SQL complejo: múltiples pasos, operaciones de escritura o lógica avanzada. |
| 9 a 10 | SQL muy complejo: muchas operaciones, alto riesgo de mantenimiento o revisión manual necesaria. |

Además del puntaje, conviene revisar las métricas individuales. Por ejemplo:

- Muchos `SELECT` pueden indicar consultas anidadas o lógica de extracción extensa.
- Muchos `UPDATE`, `DELETE` o `MERGE` pueden implicar mayor riesgo porque modifican datos.
- Muchas funciones analíticas suelen indicar lógica avanzada de ventanas, rankings o particiones.
- Muchos pasos SQL pueden indicar un proceso largo tipo ETL.

## 6. Cómo mejorar el script en el futuro

Estas son mejoras recomendadas para próximas versiones:

### Mejoras de precisión

- Usar un parser SQL real, como `sqlparse` o `sqlglot`, para distinguir mejor entre sentencias reales, subconsultas y texto dentro de strings.
- Evitar contar palabras clave que aparezcan dentro de cadenas de texto SQL, por ejemplo `'SELECT no real'`.
- Detectar `WITH` y CTEs para medir mejor consultas complejas.
- Contar `JOIN`, `GROUP BY`, `ORDER BY`, `HAVING`, `CASE WHEN` y subconsultas.

### Mejoras del puntaje

- Ajustar los pesos según experiencia del equipo.
- Separar complejidad de lectura y complejidad de escritura.
- Agregar niveles descriptivos, por ejemplo `Baja`, `Media`, `Alta` y `Crítica`.

### Mejoras de salida

- Permitir salida en JSON para integrarlo con pipelines.
- Generar un reporte en Markdown o HTML.
- Mostrar una tabla con los hallazgos.
- Permitir analizar una carpeta completa con varios archivos `.sql`.

### Mejoras de calidad

- Agregar pruebas unitarias con `pytest`.
- Configurar validaciones automáticas en GitHub Actions.
- Documentar ejemplos reales de uso.
- Agregar control de versiones semántico si el script empieza a crecer.

## 7. Flujo recomendado de trabajo

Cada vez que hagamos una mejora al proyecto, podemos seguir este flujo:

```bash
git status
git add .
git commit -m "Describe the change"
git push
```

Si se trabaja con ramas:

```bash
git checkout -b feature/nombre-de-la-mejora
git add .
git commit -m "Describe the change"
git push -u origin feature/nombre-de-la-mejora
```

Después de subir la rama, se puede abrir un Pull Request en GitHub para revisar y aprobar los cambios antes de mezclarlos a `main`.

## 8. Archivos principales del proyecto

| Archivo | Descripción |
| --- | --- |
| `sql_complexity.py` | Script principal que analiza la complejidad de SQL. |
| `DOCUMENTACION_SQL_COMPLEXITY.md` | Guía en Markdown para entender, usar y mejorar el script. |
| `README.md` | Descripción general del repositorio. |

## 9. Conclusión

Con `sql_complexity.py` tenemos una primera versión funcional para medir rápidamente la complejidad de scripts SQL. La documentación en este archivo permite que otra persona entienda cómo se creó, cómo ejecutarlo, cómo subirlo a GitHub y cómo evolucionarlo de forma ordenada.
