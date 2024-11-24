# Big Data Processing with Spark and Scala

This repository demonstrates key practices in **Big Data Processing** using **Apache Spark** and **Scala**. The project includes examples of working with DataFrames, RDDs, and User Defined Functions (UDFs) to perform common big data tasks such as filtering, joining, aggregating, and processing CSV files. The environment is set up for **IntelliJ IDEA** with a **Scala** plugin.

---

## **Features**
1. **DataFrame Operations**:
   - Create and manipulate DataFrames with Spark SQL.
   - Filter and sort records based on specific conditions.

2. **UDF Usage**:
   - Define and apply custom User Defined Functions (UDFs) for advanced transformations.

3. **DataFrame Joins**:
   - Perform joins between DataFrames to combine related datasets.
   - Calculate aggregations such as averages.

4. **RDD Operations**:
   - Count the occurrences of words in an RDD.

5. **CSV File Processing**:
   - Load and process CSV files to compute aggregated results.

---

## **Project Setup**

### **Prerequisites**
- **JDK**: Java 8 or higher
- **Scala**: Version 2.12.x
- **Apache Spark**: Version 3.2.x
- **IntelliJ IDEA**: Community or Ultimate edition with the Scala plugin installed.
- **SBT**: Scala Build Tool, version 1.5 or higher.

---

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.4",
  "org.apache.spark" %% "spark-sql" % "3.2.4",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)
### **License**
This project is licensed under the MIT License - see the LICENSE file for details.

### **Contact**
For questions or suggestions, contact with paucimi@gmail.com

### **Happy coding! 🚀**

# Procesamiento de Big Data con Spark y Scala

Este repositorio muestra prácticas clave en el **procesamiento de Big Data** utilizando **Apache Spark** y **Scala**. El proyecto incluye ejemplos de trabajo con DataFrames, RDDs y Funciones Definidas por el Usuario (UDF) para realizar tareas comunes como filtrar, unir, agregar y procesar archivos CSV. El entorno está configurado para **IntelliJ IDEA** con el plugin de **Scala**.

---

## **Características**
1. **Operaciones con DataFrames**:
   - Crear y manipular DataFrames usando Spark SQL.
   - Filtrar y ordenar registros basados en condiciones específicas.

2. **Uso de UDFs**:
   - Definir y aplicar funciones personalizadas (UDFs) para realizar transformaciones avanzadas.

3. **Uniones con DataFrames**:
   - Realizar uniones entre DataFrames para combinar conjuntos de datos relacionados.
   - Calcular agregaciones como promedios.

4. **Operaciones con RDDs**:
   - Contar las ocurrencias de palabras en un RDD.

5. **Procesamiento de archivos CSV**:
   - Cargar y procesar archivos CSV para calcular resultados agregados.
---

## **Configuración del Proyecto**

### **Requisitos Previos**
- **JDK**: Java 8 o superior.
- **Scala**: Versión 2.12.x.
- **Apache Spark**: Versión 3.2.x.
- **IntelliJ IDEA**: Edición Community o Ultimate con el plugin de Scala instalado.
- **SBT**: Scala Build Tool, versión 1.5 o superior.

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.4",
  "org.apache.spark" %% "spark-sql" % "3.2.4",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)


### Licencia
-Este proyecto está bajo la Licencia MIT. Consulta el archivo LICENSE para más detalles.

### Contacto
-Si tienes preguntas o sugerencias, contacta a paucimi@gmail.com

¡Feliz programación! 🚀
