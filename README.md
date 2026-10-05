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