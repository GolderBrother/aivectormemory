🌐 [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [English](../README.md) | Español | [Deutsch](README.de.md) | [Français](README.fr.md) | [日本語](README.ja.md)

<p align="center">
  <img src="logo.png" alt="AIVectorMemory Logo" width="200">
</p>
<h1 align="center">AIVectorMemory</h1>
<p align="center">
  <strong>Dale memoria a tu asistente de IA — Servidor MCP de memoria persistente entre sesiones</strong>
</p>
<p align="center">
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/v/aivectormemory?color=blue&label=PyPI" alt="PyPI"></a>
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/pyversions/aivectormemory" alt="Python"></a>
  <a href="https://github.com/Edlineas/aivectormemory/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-green" alt="License"></a>
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-compatible-purple" alt="MCP"></a>
</p>
---

> **¿Sigues usando CLAUDE.md / MEMORY.md como memoria?** Este enfoque de memoria basado en archivos Markdown tiene defectos fatales: el archivo crece sin parar, inyectando todo en cada sesión y consumiendo una enorme cantidad de tokens; el contenido solo permite búsqueda por palabras clave — buscar "timeout de base de datos" no encuentra "error en pool de conexiones MySQL"; compartir un archivo entre proyectos causa contaminación cruzada; no hay seguimiento de tareas, así que el progreso del desarrollo depende solo de tu cabeza; sin mencionar el truncamiento a 200 líneas, el mantenimiento manual y la imposibilidad de deduplicar o fusionar.
>
> **AIVectorMemory es un enfoque completamente diferente.** Almacenamiento en base de datos vectorial local con búsqueda semántica para recuperación precisa (coincide aunque las palabras sean diferentes), recuperación bajo demanda que solo carga memorias relevantes (consumo de tokens baja un 50%+), aislamiento multi-proyecto automático sin interferencia, y seguimiento de problemas + gestión de tareas integrado que permite a la IA automatizar completamente tu flujo de desarrollo. Todos los datos se guardan permanentemente en tu máquina — cero dependencia de la nube, nunca se pierden al cambiar de sesión o de IDE.

## ✨ Características Principales

| Característica | Descripción |
|----------------|-------------|
| 🧠 **Memoria Entre Sesiones** | Tu IA por fin recuerda tu proyecto — errores encontrados, decisiones tomadas, convenciones establecidas, todo persiste entre sesiones |
| 🔍 **Búsqueda Semántica** | No necesitas recordar las palabras exactas — busca "timeout de base de datos" y encuentra "error en pool de conexiones MySQL" |
| 💰 **Ahorro 50%+ Tokens** | Deja de copiar y pegar el contexto del proyecto en cada conversación. Recuperación semántica bajo demanda, adiós a la inyección masiva |
| 🔗 **Dev Dirigido por Tareas** | Seguimiento de problemas → desglose de tareas → sincronización de estados → archivado vinculado. La IA gestiona todo el flujo de desarrollo |
| 📊 **Panel Web** | Gestión visual de todas las memorias y tareas, red vectorial 3D para ver conexiones de conocimiento de un vistazo |
| 🏠 **Completamente Local** | Cero dependencia de la nube. Inferencia local ONNX, sin API Key, los datos nunca salen de tu máquina |
| 🔌 **Todos los IDEs** | Cursor / Kiro / Claude Code / Windsurf / VSCode / OpenCode / Trae — instalación con un clic, listo para usar |
| 📁 **Aislamiento Multi-Proyecto** | Una sola BD para todos los proyectos, aislamiento automático sin interferencia, cambio de proyecto transparente |
| 🔄 **Deduplicación Inteligente** | Similitud > 0.95 fusiona automáticamente, la base de memorias siempre limpia — nunca se desordena con el uso |

<p align="center">
  QQ群：1085682431 &nbsp;|&nbsp; 微信：changhuibiz<br>
  共同参与项目开发加QQ群或微信交流
</p>

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────┐
│                   AI IDE                         │
│  OpenCode / Claude Code / Cursor / Kiro / ...   │
└──────────────────────┬──────────────────────────┘
                       │ MCP Protocol (stdio)
┌──────────────────────▼──────────────────────────┐
│              AIVectorMemory Server               │
│                                                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────┐ │
│  │ remember │ │  recall   │ │   auto_save      │ │
│  │ forget   │ │  task     │ │   status/track   │ │
│  └────┬─────┘ └────┬─────┘ └───────┬──────────┘ │
│       │            │               │             │
│  ┌────▼────────────▼───────────────▼──────────┐  │
│  │         Embedding Engine (ONNX)            │  │
│  │      intfloat/multilingual-e5-small        │  │
│  └────────────────────┬───────────────────────┘  │
│                       │                          │
│  ┌────────────────────▼───────────────────────┐  │
│  │     SQLite + sqlite-vec (Índice Vectorial) │  │
│  │     ~/.aivectormemory/memory.db            │  │
│  └────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┘
```

## 🚀 Inicio Rápido

### Opción 1: Instalación con pip (Recomendado)

```bash
# Instalar
pip install aivectormemory

# Actualizar a la última versión
pip install --upgrade aivectormemory

# Ir al directorio de tu proyecto, configuración IDE con un clic
cd /path/to/your/project
run install
```

`run install` te guía interactivamente para seleccionar tu IDE, generando automáticamente la configuración MCP, reglas Steering y Hooks — sin configuración manual.

> **Usuarios de macOS**:
> - Si aparece el error `externally-managed-environment`, agrega `--break-system-packages`
> - Si aparece el error `enable_load_extension`, tu Python no soporta la carga de extensiones SQLite (el Python integrado de macOS y los instaladores de python.org no lo soportan). Usa Homebrew Python:
>   ```bash
>   brew install python
>   /opt/homebrew/bin/python3 -m pip install aivectormemory
>   ```

### Opción 2: uvx (sin instalación)

No necesitas `pip install`, ejecuta directamente:

```bash
cd /path/to/your/project
uvx aivectormemory install
```

> Requiere tener [uv](https://docs.astral.sh/uv/getting-started/installation/) instalado. `uvx` descarga y ejecuta el paquete automáticamente.

### Opción 3: Configuración manual

```json
{
  "mcpServers": {
    "aivectormemory": {
      "command": "run",
      "args": ["--project-dir", "/path/to/your/project"]
    }
  }
}
```

<details>
<summary>📍 Ubicación de archivos de configuración por IDE</summary>

| IDE | Ruta de configuración |
|-----|----------------------|
| Kiro | `.kiro/settings/mcp.json` |
| Cursor | `.cursor/mcp.json` |
| Claude Code | `.mcp.json` |
| Windsurf | `.windsurf/mcp.json` |
| VSCode | `.vscode/mcp.json` |
| Trae | `.trae/mcp.json` |
| OpenCode | `opencode.json` |

</details>

## 🛠️ 8 Herramientas MCP

### `remember` — Almacenar memoria

```
content (string, requerido)   Contenido en formato Markdown
tags    (string[], requerido)  Etiquetas, ej. ["error", "python"]
scope   (string)               "project" (por defecto) / "user" (entre proyectos)
```

Similitud > 0.95 actualiza automáticamente la memoria existente, sin duplicados.

### `recall` — Búsqueda semántica

```
query   (string)     Palabras clave de búsqueda semántica
tags    (string[])   Filtro exacto por etiquetas
scope   (string)     "project" / "user" / "all"
top_k   (integer)    Número de resultados, por defecto 5
```

Coincidencia por similitud vectorial — encuentra memorias relacionadas incluso con palabras diferentes.

### `forget` — Eliminar memorias

```
memory_id  (string)     ID individual
memory_ids (string[])   IDs en lote
```

### `status` — Estado de sesión

```
state (object, opcional)   Omitir para leer, pasar para actualizar
  is_blocked, block_reason, current_task,
  next_step, progress[], recent_changes[], pending[]
```

Mantiene el progreso de trabajo entre sesiones, restaura contexto automáticamente.

### `track` — Seguimiento de problemas

```
action   (string)   "create" / "update" / "archive" / "list"
title    (string)   Título del problema
issue_id (integer)  ID del problema
status   (string)   "pending" / "in_progress" / "completed"
content  (string)   Contenido de investigación
```

### `task` — Gestión de tareas

```
action     (string, requerido)  "batch_create" / "update" / "list" / "delete" / "archive"
feature_id (string)             Identificador de funcionalidad asociada (requerido para list)
tasks      (array)              Lista de tareas (batch_create, soporta subtareas)
task_id    (integer)            ID de tarea (update)
status     (string)             "pending" / "in_progress" / "completed" / "skipped"
```

Vinculado a documentos spec mediante feature_id. Update sincroniza automáticamente checkboxes de tasks.md y estado de problemas asociados.

### `readme` — Generación de README

```
action   (string)    "generate" (por defecto) / "diff" (comparar diferencias)
lang     (string)    Idioma: en / zh-TW / ja / de / fr / es
sections (string[])  Secciones específicas: header / tools / deps
```

Genera automáticamente contenido README desde TOOL_DEFINITIONS / pyproject.toml, soporte multiidioma.

### `auto_save` — Guardado automático de preferencias

```
preferences  (string[])  Preferencias técnicas expresadas por el usuario (scope=user fijo, entre proyectos)
extra_tags   (string[])  Etiquetas adicionales
```

Extrae y almacena automáticamente las preferencias del usuario al final de cada conversación, deduplicación inteligente.

## 📊 Panel Web

```bash
run web --port 9080
run web --port 9080 --quiet          # Suprimir logs de solicitudes
run web --port 9080 --quiet --daemon  # Ejecutar en segundo plano (macOS/Linux)
```

Visita `http://localhost:9080` en tu navegador. Usuario predeterminado `admin`, contraseña `admin123` (se puede cambiar en la configuración después del primer inicio de sesión).

- Cambio entre múltiples proyectos, explorar/buscar/editar/eliminar/exportar/importar memorias
- Búsqueda semántica (coincidencia por similitud vectorial)
- Eliminación de datos de proyecto con un clic
- Estado de sesión, seguimiento de problemas
- Gestión de etiquetas (renombrar, fusionar, eliminación por lotes)
- Protección por autenticación Token
- Visualización 3D de red vectorial de memorias
- 🌐 Soporte multilingüe (简体中文 / 繁體中文 / English / Español / Deutsch / Français / 日本語)

<p align="center">
  <img src="003.png" alt="Inicio de sesión" width="100%">
  <br>
  <em>Inicio de sesión</em>
</p>

<p align="center">
  <img src="dashboard-projects.png" alt="Selección de Proyecto" width="100%">
  <br>
  <em>Selección de Proyecto</em>
</p>

<p align="center">
  <img src="dashboard-overview.png" alt="Resumen y Visualización de Red Vectorial" width="100%">
  <br>
  <em>Resumen y Visualización de Red Vectorial</em>
</p>

<p align="center">
  <img src="20260306234753_6_1635.jpg" alt="Grupo WeChat" width="280">
  &nbsp;&nbsp;&nbsp;&nbsp;
  <img src="8_1635.jpg" alt="Grupo QQ: 1085682431" width="280">
  <br>
  <em>Escanea para WeChat &nbsp;|&nbsp; Escanea para QQ</em>
</p>

## ⚡ Combinación con Reglas Steering

AIVectorMemory es la capa de almacenamiento. Usa reglas Steering para indicar a la IA **cuándo y cómo** llamar estas herramientas.

Ejecutar `run install` genera automáticamente las reglas Steering y la configuración de Hooks, sin necesidad de escribirlas manualmente.

| IDE | Ubicación de Steering | Hooks |
|-----|----------------------|-------|
| Kiro | `.kiro/steering/aivectormemory.md` | `.kiro/hooks/*.hook` |
| Cursor | `.cursor/rules/aivectormemory.md` | `.cursor/hooks.json` |
| Claude Code | `CLAUDE.md` (añadido) | `.claude/settings.json` |
| Windsurf | `.windsurf/rules/aivectormemory.md` | `.windsurf/hooks.json` |
| VSCode | `.github/copilot-instructions.md` (añadido) | `.claude/settings.json` |
| Trae | `.trae/rules/aivectormemory.md` | — |
| OpenCode | `AGENTS.md` (añadido) | `.opencode/plugins/*.js` |

<details>
<summary>📋 Ejemplo de Reglas Steering (generado automáticamente)</summary>

```markdown
# AIVectorMemory - Reglas de Flujo de Trabajo

## 1. Inicio de Nueva Sesión (ejecutar en orden)

1. `recall` (tags: ["conocimiento-proyecto"], scope: "project", top_k: 100) cargar conocimiento del proyecto
2. `recall` (tags: ["preference"], scope: "user", top_k: 20) cargar preferencias del usuario
3. `status` (sin parámetro state) leer estado de sesión
4. Bloqueado → reportar y esperar; No bloqueado → entrar al flujo de procesamiento

## 2. Flujo de Procesamiento de Mensajes

- Paso A: `status` leer estado, esperar si bloqueado
- Paso B: Clasificar tipo de mensaje (chat/corrección/preferencia/problema de código)
- Paso C: `track create` registrar problema
- Paso D: Investigar (`recall` buscar errores + revisar código + encontrar causa raíz)
- Paso E: Presentar plan al usuario, establecer bloqueo esperando confirmación
- Paso F: Modificar código (`recall` buscar errores antes de cambios)
- Paso G: Ejecutar pruebas para verificar
- Paso H: Establecer bloqueo esperando verificación del usuario
- Paso I: Usuario confirma → `track archive` + desbloquear

## 3. Reglas de Bloqueo

Debe `status({ is_blocked: true })` al proponer planes o esperar verificación.
Solo desbloquear tras confirmación explícita del usuario. Nunca auto-desbloquear.

## 4-9. Seguimiento de Problemas / Verificación de Código / Gestión Spec/Tareas / Calidad de Memoria / Referencia de Herramientas / Estándares de Desarrollo

(Reglas completas generadas automáticamente por `run install`)
```

</details>

<details>
<summary>🔗 Ejemplo de Configuración de Hooks (solo Kiro, generado automáticamente)</summary>

Guardado automático al finalizar sesión eliminado. Verificación del flujo de desarrollo (`.kiro/hooks/dev-workflow-check.kiro.hook`):

```json
{
  "enabled": true,
  "name": "Verificación del Flujo de Desarrollo",
  "version": "1",
  "when": { "type": "promptSubmit" },
  "then": {
    "type": "askAgent",
    "prompt": "Principios: verificar antes de actuar, no probar a ciegas, solo marcar como completado después de pasar las pruebas"
  }
}
```

</details>

## 🇨🇳 Usuarios en China

El modelo de Embedding (~200MB) se descarga automáticamente en la primera ejecución. Si es lento:

```bash
export HF_ENDPOINT=https://hf-mirror.com
```

O agregar env en la configuración MCP:

```json
{
  "env": { "HF_ENDPOINT": "https://hf-mirror.com" }
}
```

## 📦 Stack Tecnológico

| Componente | Tecnología |
|------------|-----------|
| Runtime | Python >= 3.10 |
| BD Vectorial | SQLite + sqlite-vec |
| Embedding | ONNX Runtime + intfloat/multilingual-e5-small |
| Tokenizador | HuggingFace Tokenizers |
| Protocolo | Model Context Protocol (MCP) |
| Web | HTTPServer nativo + Vanilla JS |

## 📋 Registro de Cambios

### v1.0.3

**Optimización de búsqueda recall**
- 🔍 `recall` añade parámetro `tags_mode`: `any` (coincidencia OR) / `all` (coincidencia AND)
- 🔍 `query + tags` usa OR por defecto (cualquier etiqueta coincidente entra en candidatos), resolviendo resultados perdidos con múltiples etiquetas
- 🔍 Solo `tags` mantiene AND (navegación precisa por categoría), compatible con versiones anteriores
- 📝 Reglas de Steering actualizadas con directrices de búsqueda

### v0.2.8

**Panel Web**
- 📋 Modal de detalle de problemas archivados: clic en tarjeta archivada muestra detalles de solo lectura (todos los campos estructurados: investigación/causa raíz/solución/resultado de prueba/archivos modificados), botón rojo de eliminación en la parte inferior para eliminación permanente

**Refuerzo de Reglas Steering**
- 📝 `track create` ahora requiere campo `content` obligatorio (describir síntomas y contexto del problema), prohibido enviar solo título
- 📝 `track update` post-investigación requiere campos `investigation` y `root_cause`
- 📝 `track update` post-corrección requiere campos `solution`, `files_changed` y `test_result`
- 📝 Sección 4 añade subsección "Normas de Llenado de Campos" con campos obligatorios por etapa
- 📝 Sección 5 expandida de "Verificación de Modificación de Código" a "Verificación Pre-Operación", añade regla de recall de registros de errores antes de inicio de panel/publicación PyPI/reinicio de servicio
- 📝 `install.py` STEERING_CONTENT sincronizado con todos los cambios

**Optimización de Herramientas**
- 🔧 Descripción del campo `content` de la herramienta `track` cambiada de "contenido de investigación" a "descripción del problema (obligatorio en create)"

### v0.2.7

**Extracción automática de palabras clave**
- 🔑 `remember`/`auto_save` extraen automáticamente palabras clave del contenido y las añaden a los tags — la IA ya no necesita pasar tags completos manualmente
- 🔑 Utiliza segmentación de palabras china jieba + extracción por regex en inglés, extrayendo con precisión palabras clave de alta calidad en contenido mixto chino-inglés
- 🔑 Nueva dependencia `jieba>=0.42`

### v0.2.6

**Reestructuración de reglas Steering**
- 📝 Documento de reglas Steering reescrito de estructura de 3 secciones a 9 secciones (Inicio de sesión / Flujo de procesamiento / Reglas de bloqueo / Seguimiento de problemas / Revisión de código / Gestión de tareas Spec / Calidad de memoria / Referencia de herramientas / Estándares de desarrollo)
- 📝 Plantilla STEERING_CONTENT de `install.py` sincronizada, nuevos proyectos obtienen reglas actualizadas al instalar
- 📝 Tags cambiados de listas fijas a extracción dinámica (palabras clave extraídas del contenido), mejorando la precisión de búsqueda de memoria

**Corrección de errores**
- 🐛 Herramienta `readme` `handle_readme()` faltaba `**_`, causando error MCP `unexpected keyword argument 'engine'`
- 🐛 Corrección de paginación de búsqueda de memoria en panel web (filtrado completo antes de paginar con consulta de búsqueda, corrigiendo resultados incompletos)

**Actualizaciones de documentación**
- 📖 Cantidad de herramientas README 7→8, diagrama de arquitectura `digest`→`task`, añadidas descripciones de herramientas `task`/`readme`
- 📖 Parámetros de `auto_save` actualizados de `decisions[]/modifications[]/pitfalls[]/todos[]` a `preferences[]/extra_tags[]`
- 📖 Ejemplo de reglas Steering actualizado de formato de 3 secciones a resumen de 9 secciones
- 📖 Actualizaciones sincronizadas en 6 versiones de idiomas

### v0.2.5

**Modo de desarrollo dirigido por tareas**
- 🔗 El seguimiento de problemas (track) y la gestión de tareas (task) se conectan mediante `feature_id` en un flujo completo: descubrir problema → crear tarea → ejecutar → sincronización automática de estados → archivado vinculado
- 🔄 `task update` sincroniza automáticamente el estado del problema asociado al actualizar (todo completado→completed, en progreso→in_progress)
- 📦 `track archive` archiva automáticamente las tareas asociadas (vinculación al archivar el último problema activo)
- 📦 Nueva acción `archive` para la herramienta `task`, mueve todas las tareas del grupo funcional a la tabla de archivo `tasks_archive`
- 📊 Las tarjetas de problemas muestran el progreso de tareas asociadas (ej. `5/10`), la página de tareas soporta filtrado por archivos

**Nuevas herramientas**
- 🆕 Herramienta `task` — gestión de tareas (batch_create/update/list/delete/archive), subtareas en árbol, vinculada a documentos spec mediante feature_id
- 🆕 Herramienta `readme` — generación automática de contenido README desde TOOL_DEFINITIONS/pyproject.toml, multiidioma y comparación de diferencias

**Mejoras de herramientas**
- 🔧 `track`: nueva acción delete, 9 campos estructurados (description/investigation/root_cause/solution/test_result/notes/files_changed/feature_id/parent_id), list por issue_id para consulta individual
- 🔧 `recall`: nuevo parámetro source (manual/auto_save) y modo brief (retorna solo content+tags, ahorra contexto)
- 🔧 `auto_save`: marca memorias con source="auto_save", distingue memorias manuales de guardados automáticos

**Refactorización por separación de tablas de conocimiento**
- 🗃️ project_memories + user_memories como tablas independientes, elimina consultas mixtas scope/filter_dir, mejora de rendimiento
- 📊 DB Schema v4→v6: issues añaden 9 campos estructurados + tablas tasks/tasks_archive + campo memories.source

**Panel Web**
- 📊 Página principal con tarjeta de estado de bloqueo (rojo bloqueado/verde normal), clic para ir a la página de estado de sesión
- 📊 Nueva página de gestión de tareas (grupos funcionales plegables, filtrado por estado, búsqueda, CRUD)
- 📊 Navegación lateral optimizada (estado de sesión, problemas, tareas movidos a posición central)
- 📊 Lista de memorias con filtrado source y filtro de exclusión exclude_tags

**Estabilidad y normas**
- 🛡️ Bucle principal del servidor con captura global de excepciones, errores de mensaje individual ya no causan la caída del servidor
- 🛡️ Capa Protocol con salto de líneas vacías y tolerancia a errores de parsing JSON
- 🕐 Marcas de tiempo cambiadas de UTC a zona horaria local
- 🧹 Limpieza de código redundante (métodos no llamados, importaciones redundantes, archivos de respaldo)
- 📝 Plantilla Steering con sección de flujo Spec y gestión de tareas, reglas de continuación context transfer

### v0.2.4

- 🔇 Prompt del hook Stop cambiado a instrucción directa, eliminando respuestas duplicadas de Claude Code
- 🛡️ Reglas de Steering auto_save con protección de cortocircuito, omite otras reglas al finalizar sesión
- 🐛 Corrección de idempotencia de `_copy_check_track_script` (devolver estado de cambio para evitar falsos "sincronizado")
- 🐛 Corrección de incompatibilidad `row.get()` en issue_repo delete con `sqlite3.Row` (usar `row.keys()`)
- 🐛 Corrección de scroll en página de selección de proyectos del panel Web (no se podía desplazar con muchos proyectos)
- 🐛 Corrección de contaminación CSS del panel Web (strReplace reemplazo global corrompió 6 selectores de estilo)
- 🔄 Todos los diálogos confirm() del panel Web reemplazados por modal showConfirm personalizado (eliminar memoria/issue/etiqueta/proyecto)
- 🔄 Operaciones de eliminación del panel Web con manejo de respuesta de error API (toast en lugar de alert)
- 🧹 `.gitignore` añade regla de ignorar directorio legacy `.devmemory/`
- 🧪 Limpieza automática de residuos de proyectos temporales pytest en DB (conftest.py session fixture)

### v0.2.3

- 🛡️ Hook PreToolUse: verificación obligatoria de track issue antes de Edit/Write, rechazo si no hay issues activos (Claude Code / Kiro / OpenCode)
- 🔌 Plugin de OpenCode actualizado al formato SDK `@opencode-ai/plugin` (hook tool.execute.before)
- 🔧 `run install` despliega automáticamente check_track.sh con inyección dinámica de ruta
- 🐛 Corrección de incompatibilidad `row.get()` con `sqlite3.Row` en issue_repo archive/delete
- 🐛 Corrección de condición de carrera de session_id: lectura del último valor desde DB antes de incrementar
- 🐛 Validación de formato de fecha de track (YYYY-MM-DD) + validación de tipo issue_id
- 🐛 Refuerzo del análisis de solicitudes Web API (validación Content-Length + límite 10MB + manejo de errores JSON)
- 🐛 Corrección de lógica scope del filtro de tags (`filter_dir is not None` en lugar de verificación falsy)
- 🐛 Validación de longitud de bytes struct.unpack para exportación de datos vectoriales
- 🐛 Migración versionada del esquema (tabla schema_version + migración incremental v1/v2/v3)
- 🐛 Corrección de sincronización del número de versión `__init__.py`

### v0.2.2

- 🔇 Panel Web: parámetro `--quiet` para suprimir logs de solicitudes
- 🔄 Panel Web: parámetro `--daemon` para ejecución en segundo plano (macOS/Linux)
- 🔧 Corrección de generación de configuración MCP en `run install` (sys.executable + campos completos)
- 📋 Seguimiento de problemas CRUD y archivo (Panel Web agregar/editar/archivar/eliminar + asociación de memorias)
- 👆 Clic en cualquier parte de la fila para abrir modal de edición (memorias/problemas/etiquetas)
- 🔒 Reglas de bloqueo aplicadas en continuaciones de sesión/transferencias de contexto (requiere reconfirmación)

### v0.2.1

- ➕ Agregar proyectos desde el panel Web (explorador de directorios + entrada manual)
- 🏷️ Corrección de contaminación de etiquetas entre proyectos (operaciones limitadas al proyecto actual + memorias globales)
- 📐 Truncamiento con puntos suspensivos en paginación de modales + ancho 80%
- 🔌 OpenCode install genera automáticamente plugin auto_save (evento session.idle)
- 🔗 Claude Code / Cursor / Windsurf install genera automáticamente configuración de Hooks (guardado automático al finalizar sesión)
- 🎯 Mejoras de UX del panel Web (retroalimentación Toast, guías de estado vacío, barra de exportar/importar)
- 🔧 Clic en tarjetas de estadísticas (clic en conteos de memoria/problemas para ver detalles)
- 🏷️ Página de gestión de etiquetas distingue origen proyecto/global (marcadores 📁/🌐)
- 🏷️ Conteo de etiquetas en tarjetas de proyecto incluye etiquetas de memorias globales

### v0.2.0

- 🔐 Autenticación por Token en el panel Web
- ⚡ Caché de vectores Embedding, sin cálculo redundante para contenido idéntico
- 🔍 recall soporta búsqueda combinada query + tags
- 🗑️ forget soporta eliminación por lotes (parámetro memory_ids)
- 📤 Exportación/importación de memorias (formato JSON)
- 🔎 Búsqueda semántica en el panel Web
- 🗂️ Botón de eliminación de proyecto en el panel Web
- 📊 Optimización de rendimiento del panel Web (eliminación de escaneos completos de tabla)
- 🧠 Compresión inteligente de digest
- 💾 Persistencia de session_id
- 📏 Protección de límite de longitud de content
- 🏷️ Referencia dinámica de version (ya no codificada)

### v0.1.x

- Versión inicial: 7 herramientas MCP, panel Web, visualización 3D vectorial, soporte multilingüe

## License

Apache-2.0
