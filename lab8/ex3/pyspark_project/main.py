from pyspark.sql import SparkSession
from ReadData import ReadData
from WriteData import WriteData
from TransformData import TransformData


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Spark_lab8").getOrCreate()
    input_file = 'C:/Users/zuzan/Desktop/sem6/Big Data/lab8/ex3/spotify-2023.csv'
    output = 'C:/Users/zuzan/Desktop/sem6/Big Data/lab8/ex3/transformed_spotify-2023.csv'

    read = ReadData(spark)
    df = read.read_csv(input_file)
    df.show()

    transform = TransformData(spark)
    df_transformed = transform.transform_df(df)
    df_transformed.show()

    write = WriteData(spark)
    write.write_csv(df_transformed, output)
