# Guía ZAT: usar Codex + GitHub + VS Code desde cero

Esta guía está pensada para que cualquier persona del equipo ZAT pueda replicar un flujo completo de trabajo con **Codex**, **GitHub** y **Visual Studio Code** desde cero: crear o conectar un repositorio, pedir cambios a Codex, revisar el Pull Request y llevar el código a VS Code para seguir trabajando localmente.

> Fecha de referencia de esta guía: 2026-06-02. Las pantallas, nombres de botones y permisos pueden cambiar; ante dudas, revisar las fuentes oficiales listadas al final.

## 1. Objetivo del proceso

Al final del proceso, cada integrante debería poder:

1. Tener una cuenta y acceso a GitHub.
2. Tener un repositorio creado o clonado.
3. Conectar Codex con GitHub para trabajar sobre repositorios autorizados.
4. Pedirle a Codex tareas concretas de desarrollo, documentación, pruebas o análisis.
5. Revisar el Pull Request que Codex genere.
6. Descargar o sincronizar el repositorio en VS Code.
7. Continuar el trabajo localmente con Git: `pull`, `branch`, `commit`, `push` y Pull Requests.

## 2. Conceptos básicos

| Concepto | Explicación simple |
| --- | --- |
| Git | Herramienta para guardar versiones del código. |
| GitHub | Plataforma web donde se alojan repositorios Git y se colaboran cambios. |
| Repositorio | Carpeta versionada que contiene código, documentación y su historial. |
| Branch / rama | Línea de trabajo separada para hacer cambios sin romper `main`. |
| Commit | Foto o punto de control de cambios guardados en Git. |
| Pull Request / PR | Solicitud para revisar y fusionar cambios de una rama a otra. |
| Codex | Agente de programación de OpenAI que puede ayudar a escribir, revisar, depurar y modificar código. |
| VS Code | Editor de código local con integración de Git y GitHub. |

## 3. Prerrequisitos

Antes de empezar, cada persona del equipo debe tener:

- Cuenta de GitHub.
- Acceso al repositorio u organización de ZAT correspondiente.
- Cuenta de ChatGPT/OpenAI con acceso a Codex según el plan habilitado por la organización.
- Git instalado localmente.
- Visual Studio Code instalado.
- Permisos mínimos para clonar repositorios y crear ramas.
- Si va a administrar la conexión Codex-GitHub para toda la organización, permisos de administrador en GitHub o apoyo de alguien con esos permisos.

## 4. Flujo recomendado para ZAT

```text
Idea o requerimiento
        ↓
Crear/seleccionar repositorio en GitHub
        ↓
Conectar Codex con GitHub
        ↓
Pedir tarea a Codex con instrucciones claras
        ↓
Codex crea cambios y Pull Request
        ↓
Humano revisa, prueba y aprueba
        ↓
Clonar o actualizar repo en VS Code
        ↓
Continuar desarrollo local y subir cambios
```

## 5. Crear un repositorio en GitHub desde cero

### Opción A: crear el repo desde GitHub web

1. Entrar a GitHub.
2. Hacer clic en **New repository**.
3. Elegir la organización o cuenta propietaria, por ejemplo `ZAT`.
4. Escribir un nombre descriptivo, por ejemplo `zat-sql-tools`.
5. Definir si será **Private** o **Public**. Para trabajo interno, normalmente usar **Private**.
6. Activar `Add a README file` si se quiere iniciar con una descripción básica.
7. Crear el repositorio.

### Opción B: conectar una carpeta local ya existente

Desde la carpeta del proyecto:

```bash
git init
git status
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/ZAT/NOMBRE_REPO.git
git push -u origin main
```

Si ya existe un remoto, verificarlo con:

```bash
git remote -v
```

## 6. Preparar una estructura mínima del repositorio

Para que Codex y las personas del equipo entiendan rápido el proyecto, se recomienda tener:

```text
README.md
AGENTS.md                 # Opcional: instrucciones para agentes/Codex
src/                      # Código fuente, si aplica
tests/                    # Pruebas, si aplica
docs/                     # Documentación, si aplica
requirements.txt          # Dependencias Python, si aplica
pyproject.toml            # Configuración Python moderna, si aplica
```

### README.md mínimo recomendado

```markdown
# Nombre del proyecto

## Objetivo
Explicar qué problema resuelve.

## Cómo instalar
Comandos de instalación.

## Cómo ejecutar
Comandos principales.

## Cómo probar
Comandos de tests.

## Flujo de trabajo
Explicar ramas, PRs y responsables.
```

### AGENTS.md recomendado para ZAT

Un `AGENTS.md` ayuda a que Codex conozca reglas del repositorio.

```markdown
# Instrucciones para Codex

- Responder en español cuando el usuario escriba en español.
- Antes de modificar código, revisar README.md y la estructura del repo.
- No borrar archivos sin explicar por qué.
- Ejecutar pruebas antes de finalizar cuando existan.
- Si se cambia comportamiento funcional, documentarlo en el PR.
- Mantener cambios pequeños y revisables.
```

## 7. Conectar Codex con GitHub

Codex puede trabajar con repositorios de GitHub cuando la cuenta o workspace tiene la integración habilitada y los repositorios están autorizados.

### Para administradores o responsables de la organización

1. Confirmar que Codex esté habilitado para el workspace o plan correspondiente.
2. Entrar a la experiencia de Codex desde ChatGPT o el cliente disponible para el equipo.
3. Seleccionar **Connect to GitHub** cuando Codex lo solicite.
4. Autorizar el conector o app de GitHub para la cuenta u organización correcta.
5. Elegir qué repositorios puede usar Codex. Recomendación ZAT: autorizar solo repositorios necesarios, no todos por defecto.
6. Crear el primer ambiente o entorno de Codex seleccionando el repositorio principal.
7. Probar con una tarea pequeña, por ejemplo: “lee el README y resume el objetivo del proyecto”.

### Para integrantes del equipo

1. Entrar a Codex con la cuenta autorizada.
2. Seleccionar el repositorio de ZAT habilitado.
3. Confirmar que Codex pueda leer el repo.
4. Crear una tarea pequeña para validar el flujo.
5. Revisar el resultado antes de pedir cambios grandes.

## 8. Cómo pedirle trabajo a Codex

Codex funciona mejor con instrucciones concretas. Evitar prompts ambiguos como “arregla todo”.

### Plantilla recomendada de prompt

```text
Contexto:
- Proyecto: [nombre]
- Rama base: main
- Objetivo: [qué se quiere lograr]

Tarea:
- [cambio 1]
- [cambio 2]
- [cambio 3]

Restricciones:
- No modificar [archivo/carpeta]
- Mantener compatibilidad con [versión]
- Responder en español

Validación:
- Ejecutar [comando de prueba]
- Explicar qué se cambió
- Crear PR con resumen y pruebas
```

### Ejemplo para documentación

```text
Contexto:
Tenemos un repositorio con scripts Python para analizar SQL.

Tarea:
Crea una guía en Markdown para usuarios no técnicos que explique cómo ejecutar el script principal.

Restricciones:
No cambies el código Python.

Validación:
Revisa que el archivo Markdown tenga títulos claros, ejemplos de comandos y una sección de troubleshooting.
```

### Ejemplo para código

```text
Contexto:
El script sql_complexity.py cuenta SELECT, UPDATE, DELETE y MERGE.

Tarea:
Agrega pruebas unitarias para remove_comments, count_sql_steps y analyze_sql.

Restricciones:
No cambies la interfaz CLI.

Validación:
Ejecuta pytest y reporta el resultado.
```

## 9. Revisar el Pull Request creado por Codex

Cuando Codex genera un PR, una persona debe revisarlo antes de fusionar.

Checklist de revisión:

- ¿El cambio responde al requerimiento original?
- ¿El PR toca solo los archivos necesarios?
- ¿El resumen del PR es claro?
- ¿Hay pruebas o checks ejecutados?
- ¿Los comandos de prueba pasan?
- ¿No se subieron secretos, tokens, contraseñas o datos sensibles?
- ¿La documentación quedó actualizada?
- ¿El código es entendible para otra persona del equipo?

Si algo no está bien, comentar el PR o pedirle a Codex una corrección específica.

## 10. Volcar el repositorio a VS Code

“Volcarlo a VS Code” significa traer el repositorio de GitHub a una carpeta local y abrirlo en VS Code para trabajar desde tu máquina.

### Opción A: clonar desde VS Code

1. Abrir VS Code.
2. Abrir la paleta de comandos:
   - Windows/Linux: `Ctrl + Shift + P`
   - macOS: `Cmd + Shift + P`
3. Buscar `Git: Clone`.
4. Pegar la URL del repositorio de GitHub, por ejemplo:

```text
https://github.com/ZAT/NOMBRE_REPO.git
```

5. Elegir una carpeta local donde guardar el repo.
6. Cuando VS Code pregunte, seleccionar **Open**.
7. Si aparece la ventana de confianza del workspace, aceptar solo si el repo es conocido y confiable.

### Opción B: clonar desde terminal y abrir VS Code

```bash
git clone https://github.com/ZAT/NOMBRE_REPO.git
cd NOMBRE_REPO
code .
```

Si `code .` no funciona, instalar el comando desde VS Code:

1. Abrir la paleta de comandos.
2. Buscar `Shell Command: Install 'code' command in PATH`.
3. Cerrar y abrir la terminal nuevamente.

## 11. Traer a VS Code los cambios hechos por Codex

Si Codex ya creó un PR y fue aprobado/mergeado a `main`, actualizar tu copia local:

```bash
git checkout main
git pull origin main
```

Si quieres revisar la rama del PR antes de mergear:

```bash
git fetch origin
git checkout NOMBRE_RAMA_DEL_PR
```

También se puede usar la extensión **GitHub Pull Requests and Issues** de VS Code para revisar PRs desde el editor.

## 12. Trabajar localmente en VS Code después de Codex

Flujo recomendado:

```bash
git checkout main
git pull origin main
git checkout -b feature/mi-cambio
# editar archivos en VS Code
git status
git add .
git commit -m "Describe mi cambio"
git push -u origin feature/mi-cambio
```

Luego abrir un Pull Request desde GitHub y pedir revisión.

## 13. Buenas prácticas para el equipo ZAT

### Seguridad

- No subir secretos, API keys, tokens, contraseñas o archivos `.env`.
- Usar permisos mínimos: Codex debe acceder solo a los repositorios necesarios.
- Revisar siempre los cambios antes de mergear.
- No ejecutar scripts desconocidos sin entenderlos.

### Calidad

- Pedir cambios pequeños y específicos.
- Mantener PRs revisables.
- Agregar pruebas cuando se cambie código.
- Documentar cómo ejecutar y probar el proyecto.
- Guardar instrucciones repetibles en `README.md`, `docs/` o `AGENTS.md`.

### Trabajo con Codex

- Dar contexto claro.
- Indicar archivos que sí y no debe tocar.
- Pedir que explique cambios y pruebas.
- Pedir que no haga cambios destructivos.
- Si Codex falla, pedir una corrección puntual en vez de reiniciar con un prompt enorme.

## 14. Troubleshooting común

| Problema | Causa probable | Solución |
| --- | --- | --- |
| No veo el repositorio en Codex | Repo no autorizado o falta permiso | Pedir al admin que autorice el repo en GitHub/Codex. |
| No puedo hacer push | No tengo permisos o no estoy autenticado | Revisar acceso a GitHub y autenticación en VS Code o terminal. |
| VS Code pide login de GitHub | Acción requiere autenticación | Iniciar sesión cuando VS Code lo solicite. |
| `git pull` trae conflictos | Cambios locales chocan con cambios remotos | Revisar archivos en conflicto, resolver manualmente y commitear. |
| Codex cambió demasiados archivos | Prompt demasiado amplio | Pedir nueva iteración con alcance limitado y archivos específicos. |
| No funciona `code .` | Comando de VS Code no está en PATH | Instalar `Shell Command: Install 'code' command in PATH`. |

## 15. Checklist rápido para replicar desde cero

1. Crear cuenta GitHub.
2. Instalar Git.
3. Instalar VS Code.
4. Crear o recibir acceso al repo de ZAT.
5. Confirmar acceso a Codex.
6. Conectar Codex con GitHub.
7. Autorizar solo repos necesarios.
8. Crear tarea pequeña en Codex.
9. Revisar PR generado.
10. Clonar repo en VS Code.
11. Hacer `git pull origin main`.
12. Crear rama local para cambios propios.
13. Commit y push.
14. Abrir PR y pedir revisión.

## 16. Fuentes oficiales consultadas

- OpenAI Help Center: [Using Codex with your ChatGPT plan](https://help.openai.com/en/articles/11369540-getting-started-with-codex).
- OpenAI Help Center: [Enterprise admin getting started guide for Codex](https://help.openai.com/en/articles/11390924-placeholder).
- OpenAI Help Center: [OpenAI Codex CLI – Getting Started](https://help.openai.com/en/articles/11096431).
- GitHub Docs: [Getting started with Git](https://docs.github.com/en/get-started/learning-to-code/getting-started-with-git).
- GitHub Docs: [Pushing commits to a remote repository](https://docs.github.com/en/get-started/using-git/pushing-commits-to-a-remote-repository).
- VS Code Docs: [Working with GitHub in VS Code](https://code.visualstudio.com/docs/sourcecontrol/github).
- VS Code Docs: [Working with repositories and remotes](https://code.visualstudio.com/docs/sourcecontrol/repos-remotes).
- VS Code Docs: [Quickstart: use source control in VS Code](https://code.visualstudio.com/docs/sourcecontrol/quickstart).
