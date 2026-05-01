#!/usr/bin/env python3
"""
Scheduler worker que ejecuta tareas ETL según schedule configurado
Corre 24/7 y ejecuta jobs en horarios específicos
"""
import os
import sys
import time
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import logging

# Importar jobs
from etl.load_monthly import main as load_monthly_job

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_job(job_name: str, job_function):
    """Wrapper para ejecutar un job con logging"""
    logger.info(f"=== Iniciando job: {job_name} ===")
    start_time = datetime.now()
    
    try:
        result = job_function()
        duration = (datetime.now() - start_time).total_seconds()
        
        if result == 0:
            logger.info(f"✅ Job {job_name} completado exitosamente en {duration:.2f}s")
        else:
            logger.error(f"❌ Job {job_name} falló con código {result}")
            
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"❌ Job {job_name} falló con excepción después de {duration:.2f}s: {str(e)}")
        raise


def main():
    """Inicia el scheduler con todos los jobs configurados"""
    logger.info("=== Iniciando ETL Scheduler ===")
    logger.info(f"Timezone: UTC")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    
    # Crear scheduler
    scheduler = BlockingScheduler(timezone='UTC')
    
    # Job 1: Carga mensual
    # Ejecuta el 1er día de cada mes a las 2:00 AM UTC
    scheduler.add_job(
        func=lambda: run_job("load_monthly", load_monthly_job),
        trigger=CronTrigger(day=1, hour=2, minute=0),
        id='load_monthly',
        name='Carga mensual de campañas SCB',
        replace_existing=True
    )
    logger.info("✓ Configurado: Carga mensual (día 1 de cada mes, 2:00 AM UTC)")
    
    # TODO: Agregar los 3 jobs diarios aquí cuando los definas
    # scheduler.add_job(
    #     func=lambda: run_job("daily_job_1", daily_job_1_function),
    #     trigger=CronTrigger(hour=8, minute=0),
    #     id='daily_job_1',
    #     name='Proceso diario 1',
    #     replace_existing=True
    # )
    
    # Mostrar próximas ejecuciones
    logger.info("\n=== Próximas ejecuciones programadas ===")
    jobs = scheduler.get_jobs()
    for job in jobs:
        next_run = job.next_run_time
        logger.info(f"  {job.name}: {next_run.isoformat() if next_run else 'N/A'}")
    
    # Iniciar scheduler (blocking - corre forever)
    logger.info("\n=== Scheduler iniciado - esperando jobs ===")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("=== Scheduler detenido ===")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
