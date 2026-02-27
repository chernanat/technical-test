# Airbnb ETL Pipeline

Extrae el rating, número de reseñas y las últimas 5 reseñas de una lista de URLs de Airbnb. Opcionalmente genera insights con IA (highlight + oportunidad de mejora) usando Claude.

## Instalación

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
playwright install chromium
```

## Uso

```bash
# Sin IA
python main.py

# Con IA
python main.py
```
Y crear .env con ANTHROPIC_API_KEY=

Los resultados quedan en `output/`:
- `listings_output.csv` — una fila por listing
- `listings_output.json` — lo mismo pero con el texto completo de las reseñas
- `checkpoint.json` — permite retomar el proceso si se interrumpe

## Estructura

```
├── main.py          # pipeline principal
├── helpers.py       # funciones utilitarias
├── listings.txt     # URLs de entrada
├── requirements.txt
└── output/
```

**Playwright en vez de requests** — Airbnb es una SPA en React, un cliente HTTP normal solo recibe una página vacía. Playwright corre un browser Chromium real.

**Checkpointing** — Cada URL se guarda en `checkpoint.json` apenas termina de procesarse. Si se interrumpe y se vuelve a correr, los listings ya procesados se saltean.

## Escalabilidad a 100k URLs/día

El cuello de botella principal no es el código sino el browser (memoria/CPU) y los bloqueos por IP. El approach para escalar:

- Meter las URLs en una cola (SQS) y correr 10+ workers en paralelo, cada uno con 5 páginas concurrentes → ~50 scrapers simultáneos → ~140k páginas/día
- Rotar proxies residenciales para evitar bloqueos por IP (~1 IP cada 200 requests)
- Separar el enriquecimiento con IA del scraping y procesarlo en batch con la Batch API de Anthropic
- Persistir resultados en S3 o Bigquery diseniado para data masiva
- Cloud scheduler perfecto para programar la tarea diaria

### Prueba local de concurrencia (207 URLs)

Como validación se corrió el pipeline tres veces sobre las mismas 207 URLs variando el parámetro `CONCURRENCY`:

| CONCURRENCY | Exitosas | Sin data |
|-------------|----------|----------|
| 10          | 120      | 87       |
| 5           | 127      | 80       |
| 3           | 127      | 80       |

Con concurrencia 10 se encontraron 7 listings menos, probablemente porque al abrir demasiadas páginas en paralelo desde la misma IP Airbnb empieza a throttlear algunas requests y devuelve páginas incompletas. Con 3 y 5 los resultados fueron idénticos, lo que sugiere que en una sola máquina el punto óptimo está en ese rango. Para escalar sin perder datos la solución no es subir la concurrencia local sino distribuir la carga en más workers con IPs distintas.

### LOGS EJEMPLOS CON IA
2026-02-26 22:01:57,507 [INFO] Guardados 207 registros en output/listings_output.csv y output/listings_output.json


  Resumen del proceso:
  Total URLs procesadas : 207
  Success (Encontrada)  : 127
  Success (Sin data)    : 80
  Errors                : 0
  Blocked               : 0
  Con texto de reseñas  : 108
  Con AI insights       : 108
  Output CSV  : output/listings_output.csv
  Output JSON : output/listings_output.json


