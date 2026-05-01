# Campañas SCB - ETL Worker

Pipeline de datos para campañas SCB usando DigitalOcean App Platform Workers.

## 📋 Descripción

Este proyecto ejecuta procesos ETL automatizados para cargar datos de campañas desde GitLab (archivos parquet) a PostgreSQL.

### Procesos implementados

1. **Carga mensual** (`load_monthly.py`): 
   - Descarga `BD_CAMP_SCP_YYYYMM.parquet` desde GitLab
   - Carga ~4M registros en `stga_scp.maestra_campanias_scb_mes`
   - Ejecuta el 1er día de cada mes a las 2am UTC

2. **Procesos diarios** (pendiente definir):
   - Worker 1: [DEFINIR]
   - Worker 2: [DEFINIR]
   - Worker 3: [DEFINIR]

## 🚀 Despliegue en DigitalOcean

### Prerequisitos

1. Cuenta de DigitalOcean
2. Base de datos PostgreSQL configurada
3. Token de GitLab con acceso al repo `compartido_temporal`

### Pasos para desplegar

1. **Fork o clonar este repositorio**
   ```bash
   git clone https://github.com/llanosjp/campanias-scb-etl.git
   ```

2. **Crear App en DigitalOcean**
   - Ir a [DigitalOcean App Platform](https://cloud.digitalocean.com/apps)
   - Click en "Create App"
   - Seleccionar "GitHub" como source
   - Autorizar acceso a tu cuenta de GitHub
   - Seleccionar el repo `campanias-scb-etl`
   - DigitalOcean detectará automáticamente el archivo `.do/app.yaml`

3. **Configurar variables de entorno secretas**
   
   En la configuración de la App, agregar estos secrets:
   
   | Variable | Valor | Descripción |
   |----------|-------|-------------|
   | `GITLAB_TOKEN` | `glpat-xxx...` | Personal Access Token de GitLab |
   | `DB_HOST` | `db-postgresql-sfo3-xxx.db.ondigitalocean.com` | Host de PostgreSQL |
   | `DB_PASSWORD` | `AVNS_xxx...` | Password del usuario doadmin |

   Las demás variables ya están configuradas en `app.yaml`.

4. **Revisar y desplegar**
   - Revisar la configuración del worker
   - Click en "Create Resources"
   - DigitalOcean creará el worker y configurará el cron job

### Costos

- Worker Basic XXS: **$5/mes**
- Corre solo cuando se ejecuta el cron (mensual)
- Sin costos adicionales de compute en idle

## 🔧 Desarrollo local

### Instalación

```bash
# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Instalar dependencias
pip install -r requirements.txt
```

### Configuración local

Crear archivo `.env` con las variables:

```bash
# GitLab
GITLAB_TOKEN=glpat-xxx
GITLAB_PROJECT_ID=da1534036%2Fcompartido_temporal

# PostgreSQL
DB_HOST=db-postgresql-sfo3-xxx.db.ondigitalocean.com
DB_PORT=25060
DB_NAME=defaultdb
DB_USER=doadmin
DB_PASSWORD=AVNS_xxx
DB_SSLMODE=require

# Opcional: mes específico (formato YYYYMM)
YEAR_MONTH=202505
```

### Ejecutar carga mensual

```bash
python etl/load_monthly.py
```

## 📁 Estructura del proyecto

```
campanias-scb-etl/
├── .do/
│   └── app.yaml           # Configuración de DO App Platform
├── etl/
│   ├── __init__.py
│   └── load_monthly.py    # Script de carga mensual
├── .env.example           # Ejemplo de variables de entorno
├── .gitignore
├── requirements.txt       # Dependencias Python
└── README.md
```

## 🔄 Replicar para otro cliente

1. **Fork del repositorio** en tu cuenta de GitHub
2. **Modificar variables** en `.do/app.yaml` si es necesario:
   - `GITLAB_PROJECT_ID` si es otro repo
   - `DB_NAME` si es otra base de datos
   - Cron schedule si necesitas otra frecuencia
3. **Crear nueva App** en DigitalOcean siguiendo los pasos de despliegue
4. **Configurar secrets** con las credenciales del nuevo cliente

## 📊 Monitoreo

- **Logs**: Disponibles en DigitalOcean App Platform > App > Runtime Logs
- **Ejecuciones**: Ver historial de jobs en la pestaña "Jobs"
- **Alertas**: Configurar en Settings > Alerts (email cuando falla el job)

## 🛠️ Troubleshooting

### Error: "Variable de entorno XXX no configurada"
→ Revisar que todos los secrets estén configurados en DO App Platform

### Error: "Error descargando parquet: 404"
→ Verificar que el archivo `BD_CAMP_SCP_YYYYMM.parquet` existe en GitLab para el mes especificado

### Error: "psycopg2.OperationalError: SSL connection error"
→ Verificar que `DB_SSLMODE=require` esté configurado

### El cron no se ejecuta
→ Revisar la sintaxis del cron en `app.yaml` (formato: `minuto hora día mes día_semana`)

## 📝 Notas técnicas

- Los archivos parquet se descargan temporalmente en `/tmp` y se eliminan después de procesarse
- La carga usa batch inserts de 1000 filas para optimizar performance
- La tabla destino se reemplaza completamente en cada ejecución (DROP + CREATE)
- Se crea índice automático en `nro_documento` si la columna existe

## 📞 Soporte

Para reportar issues o solicitar features, abrir un issue en GitHub.
