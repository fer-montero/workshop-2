# Workshop-2"

## 1. Problema y objetivo analítico

**Resonance Records** es un sello discográfico mediano especializado en reediciones
de catálogo. Cada año decide qué artistas del pasado relanzar en plataformas de
streaming, pero hoy lo hace por intuición.

La dirección tiene una hipótesis: un Grammy es una señal fuerte de calidad y
trayectoria, pero no garantiza presencia en streaming. Es decir, existen
artistas con muchos reconocimientos de la Academia y una popularidad actual muy
baja en Spotify. Esos artistas serían los candidatos ideales a reedición: tienen
prestigio comprobado y un público aún por reactivar.

Para comprobarlo, el sello necesita cruzar dos fuentes que nunca se habían unido:

- **Catálogo de Spotify** (archivo CSV): popularidad, género y características
  de audio de más de 100.000 registros de canciones.
- **Historial de los Premios Grammy** (sistema relacional operacional): 4.810
  registros de reconocimientos entre 1958 y 2019.

**Objetivo:** construir un pipeline batch confiable, orquestado con Apache
Airflow y validado con Great Expectations, que alimente un Data Warehouse
dimensional capaz de identificar candidatos a reedición, los géneros donde se
concentran y su perfil sonoro.

## 2. Requerimientos analíticos

| ID | Requerimiento analítico | Datos requeridos | Fuente(s) | KPI(s) esperados | Nivel de detalle |
|---|---|---|---|---|---|
| **AR1** | ¿Qué artistas tienen un alto reconocimiento Grammy pero una baja popularidad en Spotify? Decisión: elegir artistas candidatos a reedición. | Grammy: `artist`, `year`, `category`. Spotify: `artists`, `track_id`, `popularity`. | Grammy (BD relacional) + Spotify (CSV) | **KPI1:** número de artistas candidatos a reedición y top 10 de artistas con mayor brecha entre reconocimientos y popularidad. | Artista |
| **AR2** | ¿En qué familias de género se concentran los artistas candidatos? Decisión: definir en qué géneros buscar y priorizar catálogo. | Spotify: `track_genre`, `artists`, `popularity`. Grammy: `artist` (condición de artista premiado). | Spotify (CSV) + Grammy (BD relacional) | **KPI2:** número y porcentaje de candidatos por familia de género, y popularidad promedio por familia. | Familia de género × artista |
| **AR3** | ¿Cómo es el perfil sonoro de los candidatos frente a los artistas premiados que sí son populares? Decisión: orientar el relanzamiento y la curaduría. | Spotify: `energy`, `valence`, `danceability`, `acousticness`, `explicit`, `popularity`. Grammy: `artist`, conteo de reconocimientos. | Spotify (CSV) + Grammy (BD relacional) | **KPI3:** promedio de energy, valence, danceability y acousticness, y porcentaje de canciones explícitas por segmento (candidato vs. consolidado). | Segmento de artista |

**Por qué cada requerimiento necesita ambas fuentes.** El nivel de reconocimiento
existe solo en Grammy. La popularidad, el género y el perfil sonoro existen solo
en Spotify. Ninguna fuente por sí sola puede responder la pregunta de negocio.

**Definiciones de trabajo** (los umbrales se fijarán tras el perfilado):

- *Reconocimiento Grammy:* cada registro de un artista en el archivo Grammy.
- *Popularidad del artista:* promedio de `popularity` de sus canciones únicas.
- *Candidato a reedición:* artista con reconocimientos ≥ umbral A y popularidad < umbral B.
- *Consolidado:* artista con reconocimientos ≥ umbral A y popularidad ≥ umbral B.

## 3. Alcance

**Incluye:** artistas presentes en ambas fuentes, integrados por nombre
normalizado; un hecho a grano de artista; tres KPIs y tres visualizaciones
sobre el Data Warehouse.

**No incluye:** análisis por categoría de premio, análisis antes y después de
un premio, ni recuperación de artistas desde el campo `workers`.

## 4. Supuestos y limitaciones iniciales

- La columna `winner` de Grammy vale VERDADERO en todos los registros, por lo que
  **no distingue ganadores de nominados**. Se trata cada fila como un
  "reconocimiento".
- Spotify no contiene fecha de lanzamiento. No es posible un análisis temporal
  del lado de streaming.
- La integración se hace por nombre de artista. Solo una fracción de los
  artistas Grammy aparece en Spotify, y esa cobertura se mide y se documenta.
- Una canción puede aparecer varias veces en Spotify (una por género), así que
  se deduplica por `track_id` antes de calcular promedios.

### Conciliación CSV → `grammy_source.public.grammy_awards`

Ejecución: 2026-10-05 18:59 UTC · Evidencia: `docs/evidence/source_reconciliation.json`

| Control | CSV | Base de datos | Resultado |
|---|---|---|---|
| Filas | 4.810 | 4.810 | ✅ Coincide |
| Rango de años | 1958–2019 | 1958–2019 | ✅ Coincide |
| Nulos por columna | ver tabla siguiente | ver tabla siguiente | ✅ Coinciden |

| Columna | Nulos CSV | Nulos BD | % sobre 4.810 |
|---|---|---|---|
| year | 0 | 0 | 0,00 % |
| title | 0 | 0 | 0,00 % |
| published_at | 0 | 0 | 0,00 % |
| updated_at | 0 | 0 | 0,00 % |
| category | 0 | 0 | 0,00 % |
| nominee | 6 | 6 | 0,12 % |
| artist | 1.840 | 1.840 | 38,25 % |
| workers | 2.190 | 2.190 | 45,53 % |
| img | 1.367 | 1.367 | 28,42 % |
| winner | 0 | 0 | 0,00 % |

La recarga es idempotente: ejecutar `load_grammy_source.py` dos veces mantiene 4.810 filas,
porque el contenido se reemplaza completo dentro de una transacción.

## 5. Perfilado de datos y riesgos de calidad

El perfilado se ejecuta en `notebooks/data_profiling.ipynb`. Spotify se lee del CSV y Grammy se lee de la
fuente operacional PostgreSQL (`grammy_source.grammy_awards`). El notebook no modifica ninguna fuente.
Los reportes automáticos están en `reports/` y las tablas y figuras de evidencia en `docs/evidence/profiling/`.

| ID | Dataset / Atributo | Evidencia de perfilado | Riesgo potencial de calidad | Requerimiento |
|---|---|---|---|---|
| PR01 | Grammy / `artist` | 1.840 registros sin artista (38,25 %), concentrados en 246 categorías que nunca tienen artista (premios técnicos: ingeniería, producción, notas). Ver `02_grammy_completeness.csv`, `03_grammy_missing_artist_by_category.csv` | Esos premios no se pueden atribuir a un artista; el conteo de reconocimientos por artista queda incompleto | AR1, AR2, AR3 |
| PR02 | Grammy / `artist` | 21,28 % de los valores no nulos son colaboraciones en un solo texto (featuring, &, comas). Ver `05_grammy_artist_composite_patterns.csv` | El texto compuesto no coincide con ningún artista individual de Spotify | AR1 |
| PR03 | Grammy ↔ Spotify / artista | Solo 534 de 1.658 artistas de Grammy aparecen en Spotify: 31,2 % con comparación exacta y 32,2 % normalizada. Los nombres simples coinciden en 44,1 %; las colaboraciones, en 3,5 %. Ver `08_cross_source_artist_match.csv` | Sin normalizar y separar colaboraciones se pierden candidatos; además, parte de los artistas no existe en el catálogo de Spotify (limitación de cobertura) | AR1, AR2, AR3 |
| PR04 | Spotify / `track_id` | 40.900 filas con `track_id` repetido (16.641 ids); 16.299 de esos ids aparecen con más de un género y 720 con popularidad distinta. Ver `04_spotify_uniqueness.csv` | La fuente tiene una fila por canción y género: sin tratamiento, una canción se cuenta varias veces e infla los promedios de popularidad y de audio | AR1, AR3 |
| PR05 | Spotify / `track_genre` | 114 etiquetas de género distintas. Ver `05_spotify_genres.csv` | Nivel de detalle demasiado fino para decidir en qué géneros buscar catálogo; hay que agruparlas en familias | AR2 |
| PR06 | Spotify / `artists` | 1 valor nulo; 26,38 % de las filas tienen varios artistas separados por `;`. Ver `02_spotify_completeness.csv` | Una fila sin artista no se puede cruzar; las colaboraciones deben separarse para cruzar cada artista | AR1, AR2, AR3 |
| PR07 | Spotify / `popularity` | 16.020 canciones con popularidad 0 (14,05 %); ningún valor fuera de [0, 100]. Ver `06_spotify_numeric_ranges.csv`, `fig_spotify_popularity_distribution.png` | Un 0 puede ser real o un dato ausente; sesga el umbral de "baja popularidad" | AR1, AR3 |
| PR08 | Spotify / `energy`, `valence`, `danceability`, `acousticness`, `explicit` | Ningún valor fuera de [0, 1]; `explicit` solo toma `False` (104.253) y `True` (9.747). Ver `06_spotify_numeric_ranges.csv` | Riesgo no observado en este lote; un valor fuera de escala distorsionaría el perfil sonoro (se protege como contrato de la fuente) | AR3 |
| PR09 | Spotify / `popularity` (temporal) | El dataset no tiene fecha de extracción | La popularidad es una foto de un momento; no permite analizar tendencia | AR1 (limitación) |
| PR10 | Grammy / `year` | Cobertura 1958–2019, sin años faltantes. Ver `07_grammy_temporal.csv`, `fig_grammy_records_per_year.png` | Riesgo no observado; un año fuera de rango alteraría el periodo de reconocimiento | AR1 |
| PR11 | Grammy / `winner` (fuera de alcance) | `winner = True` en los 4.810 registros | No es un defecto: el dataset solo contiene ganadores, así que cada registro equivale a un premio ganado. Define cómo se cuenta el KPI1 | AR1 |
| PR12 | Grammy / `year` + `category` + `artist` | 2 registros repiten la misma combinación. Ver `04_grammy_uniqueness.csv` | Un mismo reconocimiento podría contarse dos veces, aunque puede ser legítimo (dos obras del mismo artista premiadas en la misma categoría y año) | AR1 |

## 6. Reglas de calidad y diseño de validación

Los riesgos del perfilado (PR01–PR12) se convierten en reglas explícitas y medibles. Cada regla se justifica
con al menos uno de estos criterios: evidencia de perfilado (PRxx), requerimiento analítico (ARx),
contrato documentado de la fuente o invariante posterior a la transformación.

### 6.1 Política de severidad

| Severidad | Significado | Respuesta del pipeline | ¿Reintento? |
|---|---|---|---|
| **Critical** | Continuar o cargar no es seguro | La tarea de validación falla y bloquea todas las tareas siguientes | No: es un fallo determinista; el mismo dato vuelve a fallar |
| **Warning** | Requiere visibilidad, pero no invalida el lote | Se registra en el log y en el resultado de GX; el pipeline continúa | No |
| **Info** | Contexto de monitoreo o tendencia | Se registra para interpretación; nunca bloquea | No |

Si fallan reglas de distinta severidad en la misma validación, manda la más alta (Critical > Warning > Info).

### 6.2 Reglas de calidad

| Rule ID | Dataset / Capa | Atributo(s) | Dimensión | Regla de calidad | Métrica / Umbral | Severidad | Requerimiento | Justificación |
|---|---|---|---|---|---|---|---|---|
| DQ01 | Spotify / raw | 9 columnas en alcance | Consistencia | Existen todas las columnas requeridas y el archivo no está vacío | 9/9 columnas; filas ≥ 1 | Critical | AR1–AR3 | Contrato de la fuente |
| DQ02 | Spotify / raw | `track_id` | Completitud | Toda canción tiene identificador | 100 % no nulos | Critical | AR1, AR3 | Contrato de la fuente (llave de negocio) |
| DQ03 | Spotify / raw | `popularity` | Validez | Valor entre 0 y 100, sin nulos | 100 % en rango | Critical | AR1–AR3 | PR07 + contrato (escala 0–100) |
| DQ04 | Spotify / raw | `energy`, `valence`, `danceability`, `acousticness` | Validez | Valores entre 0 y 1 | 100 % en rango | Critical | AR3 | PR08 + contrato (escala 0–1) |
| DQ05 | Spotify / raw | `explicit` | Validez | Solo `True` o `False` | 100 % en dominio | Critical | AR3 | PR08 (base del % explícito del KPI3) |
| DQ06 | Spotify / raw | `track_genre` | Completitud | Toda canción tiene género | 100 % no nulos | Critical | AR2 | PR05 |
| DQ07 | Spotify / raw | `artists` | Completitud | La canción tiene al menos un artista | ≥ 99 % no nulos | Warning | AR1–AR3 | PR06 |
| DQ08 | Spotify / raw | `popularity` | Validez (plausibilidad) | Proporción de canciones con popularidad > 0 | ≥ 80 % | Info | AR1 | PR07 |
| DQ09 | Grammy / raw | `year`, `category`, `artist` | Consistencia | Existen las columnas requeridas y la tabla no está vacía | 3/3 columnas; filas ≥ 1 | Critical | AR1–AR3 | Contrato de la fuente |
| DQ10 | Grammy / raw | `year` | Validez | Año entre 1958 y el año en curso | 100 % en rango | Critical | AR1 | PR10 + dominio |
| DQ11 | Grammy / raw | `category` | Completitud | Todo premio tiene categoría | 100 % no nulos | Critical | AR1 | Contrato de la fuente |
| DQ12 | Grammy / raw | `artist` | Completitud | Proporción de premios con artista | ≥ 60 % no nulos | Warning | AR1–AR3 | PR01 |
| DQ13 | Grammy / raw | `year` + `category` + `artist` | Unicidad | Combinación única cuando hay artista | 0 repetidos | Info | AR1 | PR12 |
| DQ14 | Preparada | `track_id` | Unicidad | Una fila por canción | 0 duplicados | Critical | AR1, AR3 | PR04 + invariante post-transformación |
| DQ15 | Preparada | `artist_key` | Validez | Llave de artista no nula y normalizada | 100 % cumple | Critical | AR1–AR3 | PR03 + invariante post-transformación |
| DQ16 | Preparada | `genre_family` | Validez | Pertenece al catálogo de familias | 100 % en dominio | Critical | AR2 | PR05 + invariante post-transformación |
| DQ17 | Preparada | `segment`, `award_count` | Validez | `segment` ∈ {candidato, consolidado}; `award_count` ≥ 1 | 100 % cumple | Critical | AR1, AR3 | Invariante post-transformación |
| DQ18 | Preparada | `matched` | Integridad de integración | Proporción de artistas Grammy encontrados en Spotify | ≥ 25 % | Warning | AR1–AR3 | PR03 |

> Las reglas DQ14–DQ18 se confirman con el modelo dimensional (sección 7).

### 6.3 Justificación de los umbrales distintos de 100 %

Un umbral es una decisión de ingeniería: refleja el riesgo para el requerimiento, no solo el estado del lote actual.

| Regla | Umbral | Por qué |
|---|---|---|
| DQ07 | ≥ 99 % | Se observó 1 nulo. Unos pocos casos aislados no invalidan el análisis, pero superar el 1 % indicaría un problema de extracción y no un caso puntual. |
| DQ08 | ≥ 80 % | Se observó 14,05 % de canciones con popularidad 0. Un 0 puede ser real o un dato ausente; si supera el 20 %, el umbral de "baja popularidad" de AR1 queda dominado por posibles ausencias. Solo se monitorea. |
| DQ12 | ≥ 60 % | El 38,25 % de nulos se concentra en 246 categorías técnicas que nunca tienen artista por diseño de la fuente. El umbral tolera esa ausencia estructural; por debajo de 60 % la ausencia ya no se explica por la estructura. |
| DQ18 | ≥ 25 % | Se observó 32,2 % de coincidencia antes de separar colaboraciones. Una caída por debajo de 25 % indica que la normalización o la integración se rompió. No bloquea, porque la cobertura del catálogo de Spotify es una limitación conocida. |

### 6.4 Organización en Expectation Suites

| Suite | Reglas | Datos que valida | Tarea del DAG |
|---|---|---|---|
| `spotify_raw_suite` | DQ01–DQ08 | CSV de Spotify recién extraído | `validate_spotify_raw` |
| `grammy_raw_suite` | DQ09–DQ13 | Tabla de Grammy extraída de PostgreSQL | `validate_grammys_raw` |
| `prepared_suite` | DQ14–DQ18 | Datos transformados e integrados | `validate_prepared` |

Las suites raw responden: *¿estos datos pueden entrar a la transformación?* La suite preparada responde:
*¿la transformación produjo datos aptos para cargar al DW?*

### 6.5 Mapeo regla → Expectation de Great Expectations

Cada Expectation lleva el Rule ID en su metadato (`meta={"rule_id": "DQxx"}`), para rastrear cada resultado de validación hasta su regla.

| Rule ID | Expectation (GX 1.23) | Parámetros clave |
|---|---|---|
| DQ01 | `ExpectTableColumnsToMatchSet` + `ExpectTableRowCountToBeBetween` | `exact_match=False`; `min_value=1` |
| DQ02 | `ExpectColumnValuesToNotBeNull` | `column="track_id"` |
| DQ03 | `ExpectColumnValuesToNotBeNull` + `ExpectColumnValuesToBeBetween` | `min_value=0, max_value=100` |
| DQ04 | `ExpectColumnValuesToBeBetween` (una por atributo) | `min_value=0, max_value=1` |
| DQ05 | `ExpectColumnValuesToBeInSet` | `value_set=[True, False]` |
| DQ06 | `ExpectColumnValuesToNotBeNull` | `column="track_genre"` |
| DQ07 | `ExpectColumnValuesToNotBeNull` | `mostly=0.99`, `severity="warning"` |
| DQ08 | `ExpectColumnValuesToBeBetween` | `min_value=1, max_value=100, mostly=0.80`, `severity="info"` |
| DQ09 | `ExpectTableColumnsToMatchSet` + `ExpectTableRowCountToBeBetween` | `exact_match=False`; `min_value=1` |
| DQ10 | `ExpectColumnValuesToBeBetween` | `min_value=1958, max_value=<año en curso>` |
| DQ11 | `ExpectColumnValuesToNotBeNull` | `column="category"` |
| DQ12 | `ExpectColumnValuesToNotBeNull` | `mostly=0.60`, `severity="warning"` |
| DQ13 | `ExpectCompoundColumnsToBeUnique` | `ignore_row_if="any_value_is_missing"`, `severity="info"` |
| DQ14 | `ExpectColumnValuesToBeUnique` | `column="track_id"` |
| DQ15 | `ExpectColumnValuesToNotBeNull` + `ExpectColumnValuesToMatchRegex` | patrón de llave normalizada |
| DQ16 | `ExpectColumnValuesToBeInSet` | catálogo de familias de género |
| DQ17 | `ExpectColumnValuesToBeInSet` + `ExpectColumnValuesToBeBetween` | `segment`; `award_count ≥ 1` |
| DQ18 | `ExpectColumnMeanToBeBetween` | columna booleana `matched`, `min_value=0.25`, `severity="warning"` |

### 6.6 Fallo controlado (Test B)

Regla elegida: **DQ03 — `popularity` entre 0 y 100 (Critical)**.

Se genera una copia del CSV de Spotify con `popularity = 150` en una fila, sin modificar el archivo original.
Resultado esperado: `validate_spotify_raw` falla, y `transform_and_integrate`, `validate_prepared` y `load_dw`
no se ejecutan. Es un error plausible de extracción (escala distinta o dato corrupto) y la regla protege la
medida central de los tres requerimientos.