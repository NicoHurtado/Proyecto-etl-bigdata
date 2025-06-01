# Proyecto 3 - Telematica

**Curso:** Tópicos Especiales en Telemática

**Autores:** Nicolas Hurtado A. & Jacobo Restrepo M.
Nicolas Hurtado A - nhurtadoa@eafit.edu.co
Jacobo Restrepo M - jrestrep32@eafit.edu.co

**Profesor:** 
Edwin Nelson Montoya Munera
emontoya@eafit.edu.co

## 1. # Proyecto 3 - Telematica

Este proyecto implementa una canalización (pipeline) de Big Data de extremo a extremo en Amazon Web Services (AWS) para ingerir, procesar, analizar y servir datos relacionados con el clima. La canalización obtiene datos de dos fuentes principales: la API de Open-Meteo para información meteorológica histórica y una base de datos relacional (Simulada). Luego, aprovecha AWS EMR para el procesamiento distribuido con Spark para realizar operaciones ETL (Extraer, Transformar, Cargar) y hacer analisis y prediccion sobre estos. Los datos procesados se almacenan en diferentes buckets de S3 que representan zonas de datos crudos (raw), confiables (trusted) y refinados (refined). Todo el proceso está automatizado por un script orquestador basado en Python.

Aspectos Cumplidos:

*   **Ingesta de Datos:**
    *   Se obtuvieron datos meteorológicos (temperatura máxima y precipitación) a través de una API.
    *   Se obtuvieron los datos a travez de una base de datos relacional (creada)
*   **Procesamiento de Datos Escalable:** Utiliza Apache Spark en AWS EMR para ejecutar steps.
*   **Análisis Exploratorio de Datos (EDA):**
    *   El script de Spark (`scripts/spark_jobs/weather_analysis.py`) carga los datos y realiza un análisis descriptivo básico.
*   **Almacenamiento de Datos por Capas (Data Lake):** Organiza los datos en zonas Crudas, Confiables y Refinadas en Amazon S3.
*   **Orquestación Automatizada:** Un script central de Python (`pipeline_orchestrator.py`) gestiona todo el flujo de trabajo
*   **Servicio de las predicciones**
    

Aspectos con Oportunidades de Mejora:

*   **Ejecución y Robustez del Script de Automatización:**
    *   Se reportaron problemas con la ejecución repetida del script de automatización. Inicialmente funcionó, pero presentó errores en ejecuciones subsecuentes. Esto podría deberse a:
        *   Problemas de configuración del entorno ya creado.
        *   Gestión de conexiones.
        *   Permisos y Roles (Muy Importante)
*   **Servicio de las predicciones en una api unica para esto y acceso a su permiso**
    *   Aunque se generó un script SQL para la carga manual, la automatización completa del proceso ETL (Extracción desde la API, Transformación y Carga a la BD) no se abordó completamente en el script de Spark.

## 2. Características Principales

*   **Ingesta de Datos Automatizada:** Scripts para obtener datos de la API de Open-Meteo y una base de datos relacional (simulada).
*   **Procesamiento de Datos Escalable:** Utiliza Apache Spark en AWS EMR para ejecutar steps.
*   **Almacenamiento de Datos por Capas (Data Lake):** Organiza los datos en zonas Crudas, Confiables y Refinadas en Amazon S3.
*   **Orquestación Automatizada:** Un script central de Python (`pipeline_orchestrator.py`) gestiona todo el flujo de trabajo, incluyendo:
    *   Creación de cluster EMR.
    *   Gestión de scripts de arranque para clústeres EMR.
    *   Ejecución de la ingesta de datos local.
    *   Carga de trabajos de Spark a S3.
    *   Envío de trabajos de Spark como pasos de EMR y monitoreo de su finalización.
*   **Dirigido por Configuración:** Utiliza un archivo JSON (`config/buckets.json`) para gestionar los nombres y rutas de los buckets de S3.
*   **Diseño Modular de Scripts:** Scripts separados para la obtención de datos, ETL con Spark y análisis con Spark.

## 3. Estructura de Directorios

```
.Proyecto3-bigdata/
├── capture/                           
│   ├── open_meteo_fetcher.py         
│   └── db_fetcher.py                  
├── config/
│   └── buckets.json                  
├── scripts/
│   └── spark_jobs/                   
│       ├── weather_etl.py            
│       └── weather_analysis.py       
├── pipeline_orchestrator.py          
├── requirements.txt                  
└── README.md                         
```

## Guia tecnica:
Se adjunto adicionalmente una guia aun mas detallada y tecnica para este proyecto
(Guia_proyecto3_Nico_Jaco.pdf)

## 5. Cómo Ejecutar la Canalización

Una vez que todos los prerrequisitos y configuraciones estén en su lugar, puede ejecutar toda la canalización utilizando el script `pipeline_orchestrator.py` desde el directorio raíz del proyecto.

```bash
python pipeline_orchestrator.py
```

**El orquestador realizará los siguientes pasos:**
1.  Crear y cargar un script de arranque para EMR en S3.
2.  Ejecutar scripts de ingesta locales (`open_meteo_fetcher.py`, `db_fetcher.py`) para obtener datos y subirlos a los buckets S3 de datos crudos.
3.  Cargar scripts de trabajos de Spark (`weather_etl.py`, `weather_analysis.py`) al bucket de scripts de S3.
4.  Crear un nuevo clúster EMR con las configuraciones especificadas (incluyendo el script de arranque).
5.  Esperar a que el clúster EMR esté en estado 'WAITING' o 'RUNNING'.
6.  Enviar el trabajo de Spark `weather_etl.py` como un paso de EMR, pasando las rutas S3 configuradas.
7.  Esperar a que el paso ETL se complete.
8.  Enviar el trabajo de Spark `weather_analysis.py` como un paso de EMR, pasando las rutas S3 configuradas.
9.  Esperar a que el paso de análisis se complete.
10. Exponer el json como publico y accesible como un endpoint (Por permisos no pudimos crear lambda)

## 8. Resolución de Problemas y Notas Importantes

*   **Permisos de IAM:** La mayoría de los problemas suelen derivarse de permisos de IAM insuficientes para los roles utilizados por EMR (`EMR_DefaultRole`, `EMR_EC2_DefaultRole`) o el usuario/rol que ejecuta el script orquestador. Asegúrese de que puedan acceder a S3, crear/gestionar clústeres EMR y describir pasos.
*   **Gestión de Costos:** Recuerde que ejecutar clústeres EMR incurre en costos. Si `KEEP_JOB_FLOW_ALIVE` es `True`, termine manualmente el clúster EMR desde la consola de AWS después de terminar las pruebas para evitar cargos continuos. Si es `False`, el orquestador intentará la terminación, pero es una buena práctica verificarlo.

## Reflexiones y Lecciones Aprendidas

Durante la realización de este proyecto, hemos consolidado significativamente nuestra destreza en la utilización de Amazon Web Services (AWS). Adquirimos una valiosa perspectiva sobre los flujos de trabajo y el ciclo de vida que caracterizan los proyectos de Big Data, análisis de datos y Machine Learning en el ámbito industrial. Nos resultó especialmente notable la robustez y versatilidad de la arquitectura de AWS, así como la extensa gama de servicios y herramientas disponibles para afrontar estos complejos desafíos tecnológicos.


---
