package examen

import job.examen.examen
import org.apache.spark.sql.functions.{desc, udf}
import utils.TestInit

class examenTest extends TestInit {
  import spark.implicits._

  val estudiantes = Seq(
    ("Alicia", 20, 9.5),
    ("Bobby", 22, 7.8),
    ("Catalina", 21, 8.3),
    ("David", 23, 9.0),
    ("Eva", 20, 6.5)
  ).toDF("nombre", "edad", "calificacion")

  it should "verificar el esquema del DataFrame" in {
    estudiantes.printSchema()
  }

  it should "filtrar estudiantes con calificación mayor a 8" in {
    val estudiantesFiltrados = estudiantes.filter($"calificacion" > 8)
      .select("nombre", "calificacion")
      .orderBy(desc("calificacion"))

    val expectedDF = Seq(
      ("Alicia", 9.5),
      ("David", 9.0),
      ("Catalina", 8.3)
    ).toDF("nombre", "calificacion")

    checkDf(expectedDF, estudiantesFiltrados)
  }

  it should "aplicar UDF para determinar si un número es par o impar" in {
    val numeros = Seq(1, 2, 3, 4, 5, 6).toDF("numero")
    val esPar = udf((numero: Int) => if (numero % 2 == 0) "par" else "impar")
    val numerosConParidad = numeros.withColumn("paridad", esPar($"numero"))

    val expectedDF = Seq(
      (1, "impar"),
      (2, "par"),
      (3, "impar"),
      (4, "par"),
      (5, "impar"),
      (6, "par")
    ).toDF("numero", "paridad")

    checkDf(expectedDF, numerosConParidad)
  }

  it should "calcular el promedio de calificaciones por estudiante" in {
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

    val dfPromedios = examen.calcularPromedioCalificaciones(estudiantesInfo, calificaciones)

    val expectedPromedios = Seq(
      (1, "Alicia", 8.75),
      (2, "Bobby", 6.5),
      (3, "Catalina", 7.75),
      (4, "David", 9.25),
      (5, "Eva", 6.0)
    ).toDF("id", "nombre", "promedio_calificacion")

    checkDf(expectedPromedios, dfPromedios)
  }

  it should "cargar archivo CSV y calcular el ingreso total por producto" in {
    val ventas = Seq(
      ("1", "1001", 5, 20.0),
      ("2", "1002", 3, 15.0),
      ("3", "1001", 2, 20.0),
      ("4", "1003", 1, 30.0)
    ).toDF("id_venta", "id_producto", "cantidad", "precio_unitario")

    ventas.write.option("header", "true").mode("overwrite").csv("src/test/resources/ventas.csv")

    val ventasPath = "src/test/resources/ventas.csv"
    val ventasDF = examen.cargarYCalcularIngresosPorProducto(spark, ventasPath)

    val expectedIngresos = Seq(
      ("1001", 140.0),
      ("1002", 45.0),
      ("1003", 30.0)
    ).toDF("id_producto", "total_ingreso")

    checkDf(expectedIngresos, ventasDF)
  }
}