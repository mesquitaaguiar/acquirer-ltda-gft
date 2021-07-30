// importa bibliotecas
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import udf

// Método para converter String para float
def convertToFloat(s):
    try:
        return float(s)
    except ValueError:
        return float(0)

// Função UDF 
converttofloat_udf = udf(lambda str: convertToFloat(str))

// Dataframe Transação
transactionDF = spark.read.options(header='True', inferSchema='True', delimiter=';').csv("/tmp/transaction.csv")
transactionDF = transactionDF.withColumn("total_amount_dec",converttofloat_udf(col("total_amount")).cast(DecimalType(10,2))) \
                .withColumn("discount_percentage_dec",converttofloat_udf(col("discount_percentage")).cast(DecimalType(10,2)))
// Tabela Temp 
transactionDF.registerTempTable("transacao")

// Dataframe Contrato (somente os ativos)
contractActiveDF = spark.read.options(header='True', inferSchema='True', delimiter=';').csv("/tmp/contract.csv").filter(col("is_active"))
// Tabela Temp 
contractActiveDF.registerTempTable("contrato")

// Spark SQL para mostrar o valor recebido em um único comando SQL 
spark.sql("""select sum(valor) from (select cast(total * desconto * percentual as decimal(10,3)) valor from
(select total_amount_dec as total, 1 - (discount_percentage_dec/100) as desconto, percentage/100 as percentual
from transacao 
join contrato on contrato.client_id = transacao.client_id) b) a""").show()

// Resultado
// +----------+
// |sum(valor)|
// +----------+
// |   845.411|
// +----------+