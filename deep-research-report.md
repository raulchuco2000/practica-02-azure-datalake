# Solución de ingestión unificada para RetailNova en Azure  

**Resumen Ejecutivo:** Se propone una arquitectura unificada basada en Azure Data Factory (ADF) como orquestador, que ingiere datos desde *Azure SQL Database* (datos relacionales), *Azure Cosmos DB* (NoSQL documental) y archivos CSV en *Azure Blob Storage*, y los consolida en un Data Lake (ADLS Gen2) organizado en las zonas **Raw**, **Bronze (Cleansed)**, **Silver** y **Gold (Curated)**. En cada etapa los datos reciben limpiezas progresivas: la **zona Raw** almacena los datos originales con mínima transformación, la **Bronze** limpia duplicados y aplica esquemas, la **Silver** unifica entidades de negocio, y la **Gold** contiene datos listos para análisis BI. Azure Data Factory coordina pipelines que usan *Copy Activity* para mover datos, parametrizados y con manejo de errores/reintentos. Se utilizarán identidades gestionadas y RBAC para asegurar conexiones seguras, así como cifrado nativo y políticas de gobernanza (por ejemplo Azure Purview). La solución se describe detalladamente con diagramas, tablas de configuración, ejemplos de código (SQL, Bicep/Terraform) y capturas simuladas que ilustran la implementación y resultados.

## Arquitectura propuesta y justificación  

Para el flujo de datos se propone el siguiente esquema (arquitectura de medallón):  

```mermaid
flowchart LR
  subgraph Orígenes de datos
    A[Azure SQL Database<br/>(Relacional)]
    B[Azure Cosmos DB<br/>(NoSQL JSON)]
    C[Azure Blob Storage<br/>(CSV)]
  end
  A --> D[Azure Data Factory<br/>(Pipelines)]
  B --> D
  C --> D
  D --> E[ADLS Gen2: Zona Raw]
  E --> F[ADLS Gen2: Zona Bronze]
  F --> G[ADLS Gen2: Zona Silver]
  G --> H[ADLS Gen2: Zona Gold]
  H --> I[Azure Synapse/SBI<br/>(Análisis)]
```

- **Azure Data Factory** orquesta las extracciones y cargas. Como servicio ETL/Pipelines en la nube, ADF permite copiar datos entre fuentes heterogéneas de forma escalable. Admite más de 90 conectores integrados (incluyendo SQL, Cosmos, Blob) y permite integrar transformaciones cuando se requiera.

- En **Orígenes de datos**: Azure SQL DB almacena datos transaccionales estructurados; Cosmos DB (API SQL) guarda documentos JSON semi-estructurados; Azure Blob CSV contiene datos planos sin esquema. Estos flujos se unen en el Data Lake.

- **Zonas ADLS Gen2:** Se aplica un esquema *Raw → Bronze → Silver → Gold*. La **zona Raw** ingresa datos en su formato nativo con mínima transformación. En la **Bronze/Cleansed** se aplican reglas de calidad y se unifican entidades básicas. En la **Silver** los datos se integran en modelos de negocio coherentes (por ejemplo tablas unificadas de clientes, ventas, productos). Finalmente la **zona Gold/Curated** contiene datos agregados y listos para BI y ML. Se emplean formatos eficientes: por ejemplo, almacenar versiones bronze en Parquet (columna) para reducir almacenamiento y acelerar lecturas.

- **Justificación:** Esta arquitectura de medallón es un patrón ampliamente adoptado para almacenar datos gradualmente refinados. ADF es adecuado pues simplifica la ingestión desde múltiples orígenes sin necesidad de infraestructura propia. Usar un Data Lake con ADLS Gen2 con *namespace jerárquico* permite organizar zonas y aprovechar particiones (por ejemplo, particionar por fecha: `/zona/raw/ventas/yyyy/mm/dd/`) y compresión. Adicionalmente, Azure provee escalabilidad y alta disponibilidad gestionada.

## Fuentes de datos (orígenes)  

1. **Azure SQL Database (SQL Server):** Base de datos relacional. Ejemplo de tabla `Ventas` con campos `(VentaID int, Fecha date, Cliente nvarchar, Monto decimal)`. Un registro de muestra:  
   ```sql
   VentaID |   Fecha   | Cliente  | Monto
   --------+-----------+----------+-------
       100 | 2026-09-15| "Luis"   |  249.50
   ```  
   Aquí se extraerán datos de transacciones o inventarios con SQL.  

2. **Azure Cosmos DB (API SQL):** Base NoSQL orientada a documentos JSON. Ejemplo de documento en la colección `Productos`:  
   ```json
   {
     "id": "prod-ABC123",
     "nombre": "TV UltraHD",
     "categoría": "Electrónica",
     "precio": 799.99,
     "enStock": true
   }
   ```  
   Cada documento JSON puede tener estructura libre; ADF copiará o exportará estos JSON hacia el Lake.  

3. **Azure Blob Storage (CSV):** Contenedor con archivos de texto. Ejemplo de archivo CSV `clientes.csv` con cabecera `ClienteID,Nombre,Email`:  
   ```
   ClienteID,Nombre,Email
   1,María Pérez,maria@example.com
   2,Juan López,juan@example.com
   ```  
   ADF tomará estos archivos planos (CSV) y los escribirá en ADLS Raw (manteniendo el CSV sin procesar).  

## Diseño de ADLS Gen2 (zonas y formatos)  

- **Zonas y estructura de carpetas:** Por ejemplo, una posible ruta para datos se organizaría así:  
  ```
  adls://retailnova.dfs.core.windows.net/
  ├─ raw/
  │  ├─ ventas/            (datos sin procesar de SQL)
  │  ├─ productos/         (JSON originales de Cosmos)
  │  └─ clientes/          (CSV originales de Blob)
  ├─ bronze/
  │  ├─ ventas/            (datos cleansed, convertidos a Parquet)
  │  ├─ productos/         (sinonimizados, con esquema uniforme)
  │  └─ clientes/          
  ├─ silver/...
  └─ gold/...
  ```  
  Esto facilita la gobernanza y el versionado.  

- **Particiones y formatos:** En `bronze` y posteriores usar formatos columnar (parquet/ORC) para rendimiento analítico. Ejemplo de política: particionar por fecha (`/year=2026/month=09/`) para escalabilidad. La capa Raw puede mantener el formato original (JSON/CSV).

- **Beneficios:** Las zonas facilitan repetibilidad y limpieza de datos: **Raw** preserva datos originales para auditoría; **Bronze/Silver** aplica controles de calidad; **Gold** prepara datos analíticos (e.g. tablas dim/fact para reportes).

## Configuración detallada en Azure Data Factory  

En ADF se definen **Linked Services**, **Datasets** y **Pipelines**:  

- **Linked Services (Conexiones):** Especifican cómo conectarse a cada recurso. En Data Factory Studio vamos a *Manage > Linked services* y creamos uno nuevo por fuente. Por ejemplo, para Cosmos DB:  
  1. Hacer clic en **+ New** en la pestaña *Linked Services*.  
  2. Buscar y seleccionar el conector **Azure Cosmos DB for NoSQL**.  
      *Figura: Selección del conector “Azure Cosmos DB for NoSQL” al crear un Linked Service en ADF.*  
  3. Configurar detalles: introducir el *AccountEndpoint* y *AccountKey* (o mejor, usar identidad gestionada/Key Vault para la clave) y probar la conexión. Se crea el servicio. El siguiente ejemplo JSON ilustra el Linked Service (API SQL) con la cadena de conexión:  
     ```jsonc
     {
       "name": "LS_CosmosProd",
       "properties": {
         "type": "CosmosDb",
         "typeProperties": {
           "connectionString": "AccountEndpoint=https://<cosmos-url>;AccountKey=<key>;Database=RetailNova"
         }
       }
     }
     ```  
      *Figura: Configuración del Linked Service para Azure Cosmos DB (introducir endpoint y credenciales).*  

  De forma similar se crean Linked Services para: Azure SQL Database (usando autenticación SQL o Managed Identity) y Azure Blob/ADLS Gen2. En cada caso se escoge el conector adecuado (por ejemplo *Azure SQL Database* o *Azure Data Lake Storage Gen2*) y se configuran cadenas de conexión o identidades.  

- **Datasets (Conjuntos de datos):** Definen la fuente/destino de datos. Por cada Linked Service creamos un Dataset:  
  - *SQL Database (Tabla):* dataset tipo “Azure SQL Table” que apunta a la tabla `Ventas` en RetailNovaDB (LinkedService=LS_SQL).  
  - *Cosmos DB (Colección):* dataset tipo “Azure Cosmos DB for NoSQL” apuntando a la colección `Productos`.  
  - *Blob Storage (CSV):* dataset tipo “DelimitedText” enlazado al contenedor de blobs donde están los CSV, con la ruta del archivo (por ejemplo `raw/clientes/*.csv`).  

  En cada dataset se definen esquemas (por ejemplo columnas para CSV/SQL), formato (delimitador, codificación). Esto permite a Copy Activity interpretar correctamente cada fuente.  

- **Pipelines y Copy Activity:** Se crearán pipelines para cada flujo, típicamente uno por origen, o uno general con múltiples actividades. Por ejemplo, un pipeline **Ingesta-Ventas** con una *Copy Activity* que lee de la tabla `Ventas` en SQL (Dataset SQL) y escribe a ADLS Raw (/raw/ventas). Otro pipeline similar para Cosmos y Blob. Cada Copy Activity puede incluir *mapeos* de columna (si las columnas no coinciden exactamente) o usar modo de copia automática si son iguales.  

  - *Parámetros:* Los pipelines pueden parametrizar nombres de contenedor, fechas de ingesta, etc., reutilizando el mismo pipeline para diferentes tablas o días.  
  - *Triggers:* Se definirán desencadenadores de pipeline (por ejemplo triggers programados diarios o triggers basados en evento de llegada de blob). Por ejemplo, un **Schedule Trigger** diario a medianoche para cargar las ventas del día, o un **Event Trigger** que dispare la ingestión cuando aparece un archivo CSV en el contenedor raw.  
  - *Reintentos y manejo de errores:* En las propiedades de cada actividad se configura un número de reintentos (e.g. 3), con retraso exponencial. Se puede usar ramas condicionales “**On Failure**” para notificar o hacer limpieza de errores. Por ejemplo, capturar errores de tiempo de espera o conexión y enviar alertas por correo.  

  Como ejemplo, la siguiente figura muestra un pipeline de ADF con una actividad *Copy Data*. En este caso el pipeline “CopyPipeline” arrastra la actividad *Copy CSV*:  

   *Figura: Ejemplo de pipeline en Azure Data Factory con una actividad “Copy Data” configurada.*  

  En este ejemplo de ADF se arrastra la actividad *Copy Data* desde la caja de herramientas, se asigna el dataset de origen (e.g. CSV) y destino, y se publica el pipeline. Según la documentación de Microsoft, “arrastrar y soltar Copy Data” es el modo típico de construir canalizaciones.  

- **Mapeos de datos:** Si los formatos no coinciden, ADF permite definir mapeos columna a columna en la actividad Copy. Por ejemplo, al copiar de SQL a Parquet podemos mapear `ClienteID -> id_cliente`, `Monto -> monto_vta`, etc. Esto se puede hacer en la pestaña *Mappings* de la actividad.

## Scripts SQL y ejemplos de extracción  

- **SQL Database:** Para extraer de SQL se usará una consulta básica. Ejemplo de script para volcados completos:  
  ```sql
  SELECT VentaID, Fecha, Cliente, Monto
  FROM dbo.Ventas
  WHERE Fecha >= '2026-09-01'
  ```  
  Este script se puede usar en un *Query* source del Linked Service SQL en el Copy Activity, o bien se crea un *Dataset* SQL Table y ADF se encarga de leer toda la tabla.

- **Azure Cosmos DB (NoSQL):** Se pueden realizar consultas SQL en Cosmos (API SQL) si es necesario filtrar datos. Ejemplo:  
  ```sql
  SELECT c.id, c.nombre, c.precio
  FROM c
  WHERE c.enStock = true
  ```  
  Pero más comúnmente se copiarán los documentos JSON completos sin transformación. En ADF, el Copy puede importar los JSON tal cual hacia archivos en ADLS (`.json`) o escribirlos en una tabla intermedia.

- **CSV / Blob:** Los archivos CSV ya son texto plano; no requieren un script. Un ejemplo de contenido CSV de clientes fue mostrado arriba. Se mantendrá la estructura original en la zona Raw y en Bronze se podría convertir a Parquet usando un Data Flow o Spark (opcional).

En la capa **Silver/Gold**, podrían emplearse transformaciones avanzadas (por ejemplo stored procedures en SQL o Azure Databricks notebooks) para unir tablas y generar tablas analíticas, pero eso queda fuera del alcance de ingestión pura.

## Aprovisionamiento de recursos en Azure (Bicep/ARM/Terraform)  

Se recomienda infraestructuras como código para reproducir el entorno. Ejemplo de Bicep básico para crear los servicios principales:  

```bicep
// Crear cuenta de almacenamiento para ADLS Gen2
resource storage 'Microsoft.Storage/storageAccounts@2021-09-01' = {
  name: 'retailnovaadls'
  location: resourceGroup().location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true           // Habilita espacio de nombres jerárquico
    encryption: {
      services: { blob: {} file: {} }
      keySource: 'Microsoft.Storage'
    }
  }
}

// Crear servidor SQL y base de datos
resource sqlServer 'Microsoft.Sql/servers@2022-08-01' = {
  name: 'retailnovasql'
  location: resourceGroup().location
  properties: { administratorLogin: 'sqladmin', administratorLoginPassword: 'ComplexPa$$w0rd' }
}
resource sqlDB 'Microsoft.Sql/servers/databases@2022-08-01' = {
  name: '${sqlServer.name}/RetailNovaDB'
  properties: { collation: 'SQL_Latin1_General_CP1_CI_AS' }
}

// Crear instancia de Cosmos DB (Core, SQL API)
resource cosmos 'Microsoft.DocumentDB/databaseAccounts@2021-10-15' = {
  name: 'retailnovacosmos'
  location: resourceGroup().location
  kind: 'GlobalDocumentDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    locations: [{ locationName: resourceGroup().location }]
    enableFreeTier: true
    consistencyPolicy: { defaultConsistencyLevel: 'Session' }
  }
}

// Crear Data Factory con identidad gestionada (service principal)
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: 'retailnova-adf'
  location: resourceGroup().location
  identity: { type: 'SystemAssigned' }
}
```  
En Terraform o ARM templates sería equivalente (definir los recursos con la sintaxis respectiva). Estos ejemplos crean ADLS Gen2, SQL Server/DB, Cosmos DB y Data Factory con identidad gestionada. A partir de aquí se podrían enlazar variables o parámetros para ubicar los recursos en módulos separados.

## Pruebas de ingestión, validaciones y métricas  

Con datos de ejemplo pequeños (el escenario no especifica escala), las pruebas confirmarían que ADF copia datos correctamente a cada zona. Se monitorearía lo siguiente:  
- **Latencia y rendimiento:** Azure Data Factory puede aprovechar hasta ~5 Gbps de throughput de red, por lo que incluso con grandes volúmenes (GB/TB diarios) las cargas son rápidas. Por ejemplo, el uso de un *Integration Runtime* estándar puede mover cientos de MB/s. La latencia (tiempo de carga) dependerá del tamaño de datos y del rendimiento configurado. Se mediría el *delay* de cada pipeline: desde que arranca hasta que finaliza la copia.  

- **Validaciones de datos:** Se puede usar el *Data Preview* de ADF para verificar filas copiadas, y validaciones en destino (por ejemplo, contar filas en ADLS vs. fuente). Errores comunes: credenciales inválidas, firewall de origen que bloquea IP de ADF, formatos CSV inválidos (se recomienda manejo de errores para saltar líneas con mal formateo). ADF generará logs de ejecución donde se verá si faltan columnas o hay timeout en el conector.  

- **Errores típicos:** Conexión rechazada (por IP/IP público cerrado), permisos insuficientes (suele pasar si no se otorga rol a la identidad ADF), y desbordes de memoria si el dataset es masivo (en cuyo caso usar particiones o paralelismo). Por ejemplo, copiar un archivo CSV muy grande puede requerir dividirlo o activar paralelismo en Copy Activity (definido en *Performance*).  

## Capturas simuladas / monitores  

En la siguiente figura se muestra el listado de Linked Services en ADF (administración) donde aparece el nuevo servicio de Cosmos DB creado: . Esto demostraría que la conexión fue definida exitosamente.  

Además, el portal de Azure y las herramientas de Data Factory ofrecen secciones de *Monitor*. Un ejemplo hipotético: suponiendo que corremos el pipeline, veríamos en el panel de Monitor todas las ejecuciones. Por ejemplo, la captura simulada siguiente podría mostrar una ejecución exitosa de cada actividad (verde) y el resumen de filas copiadas:  

```
+-------------+----------------------+------------+--------+
| Pipeline    | RunStart             | RunEnd     | Status |
+-------------+----------------------+------------+--------+
| CopyPipeline| 2026-09-25T10:00:00Z | 10:00:05Z  | Éxito  |
| CopyCSV     | 2026-09-25T10:00:01Z | 10:00:04Z  | Éxito  |
+-------------+----------------------+------------+--------+
```

Finalmente, se comprobaría en Azure Storage Explorer (o portal) que bajo `adls://retailnova` las carpetas `/raw/ventas/`, `/raw/productos/`, `/raw/clientes/` contienen los archivos originales, y en `/bronze/` los archivos Parquet generados. También verificaríamos filas en SQL DB o documentos en Cosmos (por ejemplo con Azure Data Studio o Data Explorer) tras la ingestión.  

## Recomendaciones operativas y de seguridad  

- **Identidades gestionadas y RBAC:** Usar la identidad gestionada asignada al Data Factory (o crear una de usuario) para autenticar contra SQL, Cosmos y ADLS. Esto evita credenciales en texto plano. Se deben otorgar permisos mínimos: por ejemplo, rol *Reader/Contributor* sobre el Storage, y en SQL DB permisos de sólo lectura (o un rol específico) para ADF. En Cosmos, asignar el rol *Cosmos DB Built-in Data Contributor* a la identidad. Así, ADF usa Azure AD para conectarse. En el ejemplo JSON de Linked Service se podría usar `"authentication": "MSI"` en vez de la cadena con clave.  

- **Cifrado:** Todos los servicios Azure utilizan cifrado en reposo por defecto. Para ADLS Gen2 se puede optar por claves propias (CMK) vinculadas a Key Vault. Transport Layer Security (TLS) protege los datos en tránsito. Habilite siempre *HTTPS* y considere configurar *Private Endpoints* o redes virtuales para aislar el acceso, especialmente para Cosmos DB y SQL (firewall + VNet rules).  

- **Monitoreo y gobernanza:** Activar diagnosticos en ADF y enviar logs a Log Analytics para alertas (por ejemplo, envíos fallidos). Registrar auditoría en SQL (Azure SQL Audit) y en Cosmos (Azure Monitor). Para gobernanza, emplear Azure Purview o Data Catalog para etiquetar los datasets, definir linaje y políticas de retención.  

## Opciones alternativas y comparativa  

| Opción             | Pros                                                               | Contras                                                          |
|--------------------|--------------------------------------------------------------------|------------------------------------------------------------------|
| **Azure Data Factory** (propuesta) | · Servicio ETL nativo sin infraestructura propia. <br>· +90 conectores integrados; ideal para ingesta escalable. <br>· Orquestación visual y parametrizable; escalable bajo demanda. | · Las transformaciones complejas requieren Data Flows o servicios externos (p.ej. Databricks). <br>· Licenciamiento por actividad (puede subir costos en volúmenes muy altos). |
| **Azure Databricks** | · Motor Spark optimizado para big data/AI. <br>· Excelente para transformaciones avanzadas, streaming y ML. <br>· Admite código (Python/SQL) y notebooks colaborativos. | · Requiere mayor complejidad operativa (cluster management). <br>· Costos de clúster, configuración y mantenimiento superiores. <br>· Menos “sin código” que ADF para simples cargas. |
| **Azure Synapse Analytics** (Pipelines) | · Integración con Data Lakehouse, Spark pools y data warehousing. <br>· Parche todo en un solo producto (SQL pool, Spark, ADF). <br>· Escalado elástico; incluye monitoring unificado. | · Más complejo de gestionar que ADF standalone. <br>· Sobre-dimensionado si solo se necesita ingestión, además de implicar costos adicionales por pools. |
| **Azure Logic Apps** | · Fácil de usar para integraciones sencillas y flujos de trabajo de negocio. <br>· Muchos conectores SaaS y para flujos de eventos. <br>· Óptimo para casos de baja escala o integración de servicios (emails, notificaciones). | · No está diseñado para cargas masivas de datos (límite de throughput y filas). <br>· Más costoso para grandes volúmenes comparado con ADF. <br>· No soporta transformaciones analíticas avanzadas. |

> **Conclusión comparativa:** Para cargas por lotes de datos estructurados y no estructurados a gran escala, ADF es generalmente la opción más adecuada (gestión bajo demanda, integración nativa). Databricks/Synapse son mejores si además se requieren análisis complejos o ML en el proceso. Logic Apps se orienta a integraciones ligeras de aplicaciones, no a ETL de datos a gran escala.

## Entregables  

El resultado final incluye:  
- **Documento técnico en Markdown** (formato listo para PDF), en español, con la explicación detallada.  
- **Esquemas y diagramas** (Mermaid) incluidos en el texto.  
- **Archivos de scripts** SQL de ejemplo (como en la sección anterior) y plantillas Bicep/Terraform (fragmentos como los mostrados) para la creación de recursos en Azure.  
- **Capturas de pantalla simuladas (PNG)** que acreditan cada paso: interfaz de ADF (Linked Services, pipelines) y ejemplo de monitoreo/resultados. (En el documento se muestran usando `embed_image` con explicaciones).

Todas las configuraciones y ejemplos se fundamentan en documentación oficial de Microsoft y mejores prácticas reconocidas.