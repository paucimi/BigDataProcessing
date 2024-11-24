package job.examen

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object examen {
  def main(args: Array[String]): Unit = {
    // Iniciar la sesión de Spark
    val spark = SparkSession.builder()
      .appName("ExamenApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Tarea 1: DataFrame de estudiantes
    val estudiantes = Seq(
      ("Alicia", 20, 9.5),
      ("Gabriel", 22, 7.8),
      ("Pedro", 21, 8.3),
      ("David", 23, 9.0),
      ("Ramon", 20, 6.5),
      ("Natalia", 18, 5.2),
      ("Lucía", 18, 9.1),
      ("Luis", 19, 8.4),
      ("Ricardo", 24, 8.2),
      ("Cristina", 20, 5.8),
      ("Patricia", 22, 5.4),
      ("Paula", 25, 6.0),
      ("Iván", 21, 8.4),
      ("Javier", 25, 6.1),
      ("Miguel", 23, 6.9),
      ("María", 21, 6.8)
    ).toDF("nombre", "edad", "calificacion")

    estudiantes.printSchema()

    val estudiantesFiltrados = estudiantes
      .filter($"calificacion" > 8)
      .select("nombre", "calificacion")
      .orderBy(desc("calificacion"))

    estudiantesFiltrados.show()

    // Tarea 2: UDF para determinar si un número es par o impar
    val numeros = Seq(1, 2, 3, 4, 5, 6).toDF("numero")
    val esPar = udf((numero: Int) => if (numero == null) "nulo" else if (numero % 2 == 0) "par" else "impar")
    val numerosConParidad = numeros.withColumn("paridad", esPar($"numero"))
    numerosConParidad.show()

    // Tarea 3: Join entre DataFrames de estudiantes y calificaciones
    val estudiantesInfo = Seq(
      (1, "Alicia"),
      (2, "Bobby"),
      (3, "Catalina"),
      (4, "David"),
      (5, "Eva")
    ).toDF("id", "nombre")

    val calificaciones = Seq(
      (1, "Matemáticas", 9.0),
      (1, "Historia", 8.5),
      (2, "Matemáticas", 6.0),
      (2, "Historia", 7.0),
      (3, "Matemáticas", 7.5),
      (3, "Historia", 8.0),
      (4, "Matemáticas", 9.5),
      (4, "Historia", 9.0),
      (5, "Matemáticas", 5.5),
      (5, "Historia", 6.5)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    val promediosCalificaciones = estudiantesInfo
      .join(calificaciones, $"id" === $"id_estudiante", "left_outer")
      .groupBy("id", "nombre")
      .agg(avg("calificacion").alias("promedio_calificacion"))

    promediosCalificaciones.na.fill(0.0, Seq("promedio_calificacion")).show()

    // Tarea 4: Contar ocurrencias de palabras en un RDD
    val palabras = Seq("manzana", "banana", "manzana", "pera", "banana", "manzana")
    val palabrasRDD: RDD[String] = spark.sparkContext.parallelize(palabras)

    if (palabrasRDD.isEmpty()) {
      println("El RDD está vacío.")
    } else {
      val conteoPalabras = palabrasRDD
        .map(palabra => (palabra, 1))
        .reduceByKey(_ + _)
        .toDF("palabra", "conteo")

      conteoPalabras.show()
    }

    // Tarea 5: Cargar archivo CSV y calcular el ingreso total por producto
    val ventasPath = "resources/examen/ventas.csv"
    val ventasDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(ventasPath)

    val ingresosPorProducto = ventasDF
      .withColumn("ingreso_total", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(sum("ingreso_total").alias("total_ingreso"))

    ingresosPorProducto.show()

    // Parar la sesión de Spark
    spark.stop()
  }

  def calcularPromedioCalificaciones(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    val joinedDf = estudiantes.join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
    val promedios = joinedDf.groupBy("id", "nombre")
      .agg(avg("calificacion").alias("promedio_calificacion"))
    promedios.na.fill(0.0, Seq("promedio_calificacion"))
  }

  def cargarYCalcularIngresosPorProducto(spark: SparkSession, filePath: String): DataFrame = {
    val ventasDF = spark.read.option("header", "true").csv(filePath)
    ventasDF.withColumn("ingreso_total", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(sum("ingreso_total").alias("total_ingreso"))
  }
}
