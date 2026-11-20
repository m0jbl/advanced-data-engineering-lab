"""Resolve the lab's connector jars once at build time and add the ones PySpark does not already ship."""
import glob, os, re, shutil
from pyspark.sql import SparkSession

PACKAGES = os.environ["LAB_SPARK_PACKAGES"]
EXCLUDES = os.environ.get("LAB_SPARK_EXCLUDES", "")
spark = (SparkSession.builder.master("local[1]")
         .config("spark.jars.packages", PACKAGES)
         .config("spark.jars.excludes", EXCLUDES)
         .config("spark.jars.ivy", "/tmp/ivy").getOrCreate())
spark.stop()

jars_dir = os.path.join(os.environ["SPARK_HOME"], "jars")
shipped = {re.sub(r"-\d[\w.\-]*\.jar$", "", os.path.basename(j)) for j in glob.glob(f"{jars_dir}/*.jar")}
for jar in glob.glob("/tmp/ivy/jars/*.jar"):
    artifact = re.sub(r"-\d[\w.\-]*\.jar$", "", os.path.basename(jar).split("_", 1)[1])
    if artifact not in shipped:
        shutil.copy(jar, jars_dir)
        print("added", os.path.basename(jar))
shutil.rmtree("/tmp/ivy")
