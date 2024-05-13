class WriteData:
    def __init__(self, spark):
        self.spark = spark

    def write_csv(self, df, filepath):
        return df.write.csv(filepath, header=True, mode='overwrite')
