import os
import shutil
import time
from contextlib import contextmanager

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

Weblog_dir = os.getcwd()
CSV_saved_dir = os.getcwd()
CSV_folder_name = "CSV_data"

@contextmanager
def timer(message):
    tick = time.time()
    yield
    tock = time.time()
    print("%s: %.3f" % (message, (tock - tick)))

def find_Weblog(path=os.getcwd()):
    Weblog_files = [f for f in os.listdir(path) if os.path.isfile(f) and f.startswith("Weblog")]
    return Weblog_files

# Write DataFrame into CSV file. if the Folder has existed, it would be removed.
def write_CSV(df, path=os.getcwd(), name="CSV_data"):
    full_dir = os.path.join(path, name)
    if os.path.exists(full_dir):
        shutil.rmtree(full_dir)
    df.write.csv(full_dir)

def main():
    conf = SparkConf().setMaster("local").setAppName("Task 3") \
        .set("spark.debug.maxToStringFields", 100)
    sc = SparkContext(conf=conf)

    files = find_Weblog(Weblog_dir)
    df = SQLContext(sc).read.json(files)
    df2 = df.select("captcha_id", F.to_timestamp(F.substring("request_time", 1, 19)).alias("request_time")) \
        .filter((df.captcha_id != "null") & (df.request_time != "null")) \
        .groupBy("captcha_id", "request_time") \
        .count() \
        .orderBy("captcha_id", "request_time")
    write_CSV(df2, path=CSV_saved_dir, name=CSV_folder_name)

if __name__ == "__main__":
    with timer("Running"):
        main()
