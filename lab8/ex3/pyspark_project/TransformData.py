import pyspark.sql.functions as F

class TransformData:
    def __init__(self, spark):
        self.spark = spark

    def transform_df(self, df):
        df_transformed = df.withColumn("Tempo",
                                       F.when(df["bpm"] < 20, "Very Slow")
                                        .when((df["bpm"] >= 20) & (df["bpm"] < 70), "Slow")
                                        .when((df["bpm"] >= 70) & (df["bpm"] < 90), "Medium-Slow")
                                        .when((df["bpm"] >= 90) & (df["bpm"] < 110), "Medium")
                                        .when((df["bpm"] >= 110) & (df["bpm"] < 130), "Medium-Fast")
                                        .otherwise("Fast"))
        return df_transformed
