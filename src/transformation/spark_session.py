"""
Module: spark_session
Description: Centralized factory for creating and configuring the Apache Spark Session.
             Handles Windows Hadoop/winutils environment configuration, Java runtime
             discovery, loopback driver binding, and memory tuning from config.yaml.
"""

import os
import sys
import yaml
from pyspark.sql import SparkSession

# Ensure project root is in sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

logger = get_logger("spark_session_factory")

_SPARK_INSTANCE = None


def setup_environment():
    """Configures JAVA_HOME, HADOOP_HOME, PYSPARK_PYTHON, and PATH for Windows Spark compatibility."""
    # Ensure Spark workers use the exact virtualenv Python executable
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Configure HADOOP_HOME for Windows winutils
    hadoop_dir = os.path.join(PROJECT_ROOT, "hadoop")
    hadoop_bin = os.path.join(hadoop_dir, "bin")

    if os.name == "nt":
        if os.path.exists(hadoop_bin):
            os.environ["HADOOP_HOME"] = hadoop_dir
            if hadoop_bin not in os.environ.get("PATH", ""):
                os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
            logger.info(f"Configured HADOOP_HOME on Windows: {hadoop_dir}")
        else:
            logger.warning(f"Hadoop bin directory not found at: {hadoop_bin}")

    # Ensure JAVA_HOME points to a valid Java root containing bin\java.exe
    if "JAVA_HOME" not in os.environ or not os.path.exists(os.path.join(os.environ.get("JAVA_HOME", ""), "bin", "java.exe")):
        candidate_java_paths = [
            r"C:\Program Files\Java\jre1.8.0_503",
            r"C:\Program Files\Java\latest\jre-1.8",
            r"C:\Program Files\Java\jdk-17",
            r"C:\Program Files\Java\jdk-11",
        ]
        for p in candidate_java_paths:
            java_exe = os.path.join(p, "bin", "java.exe")
            if os.path.exists(java_exe):
                os.environ["JAVA_HOME"] = p
                logger.info(f"Auto-detected and configured JAVA_HOME: {p}")
                break


def load_spark_config() -> dict:
    """Reads Spark settings from config/config.yaml."""
    config_path = os.path.join(PROJECT_ROOT, "config", "config.yaml")
    defaults = {
        "app_name": "Olist_Ecommerce_ETL",
        "master": "local[*]",
        "driver_memory": "4g",
        "executor_memory": "4g",
        "shuffle_partitions": 8,
        "log_level": "WARN",
    }
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
                return cfg.get("spark", defaults)
        except Exception as e:
            logger.warning(f"Could not load Spark config from YAML: {e}. Using defaults.")
    return defaults


def get_spark_session(app_name: str = None) -> SparkSession:
    """
    Returns a configured SparkSession singleton.
    """
    global _SPARK_INSTANCE

    if _SPARK_INSTANCE is not None:
        return _SPARK_INSTANCE

    setup_environment()
    cfg = load_spark_config()

    session_name = app_name or cfg.get("app_name", "Olist_Ecommerce_ETL")
    master = cfg.get("master", "local[*]")
    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY") or cfg.get("driver_memory", "4g")
    exec_mem = os.environ.get("SPARK_EXECUTOR_MEMORY") or cfg.get("executor_memory", "4g")
    shuffle_parts = str(cfg.get("shuffle_partitions", 8))
    log_level = cfg.get("log_level", "WARN")

    builder = (
        SparkSession.builder
        .appName(session_name)
        .master(master)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", driver_mem)
        .config("spark.executor.memory", exec_mem)
        .config("spark.sql.shuffle.partitions", shuffle_parts)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )

    hadoop_dir = os.environ.get("HADOOP_HOME")
    if hadoop_dir:
        builder = builder.config("spark.hadoop.home.dir", hadoop_dir)

    _SPARK_INSTANCE = builder.getOrCreate()
    _SPARK_INSTANCE.sparkContext.setLogLevel(log_level)

    logger.info(f"SparkSession created: version={_SPARK_INSTANCE.version}, master={master}")
    return _SPARK_INSTANCE


def verify_spark_environment():
    """Diagnostics helper printing environment status and compatibility info."""
    setup_environment()
    cfg = load_spark_config()
    spark = get_spark_session()

    java_home = os.environ.get("JAVA_HOME", "Unset")
    hadoop_home = os.environ.get("HADOOP_HOME", "Unset")

    print("\n" + "=" * 60)
    print("PYSPARK & JAVA RUNTIME ENVIRONMENT VERIFICATION")
    print("=" * 60)
    print(f"Spark Version               : {spark.version}")
    print(f"Python Version              : {sys.version.split()[0]}")
    print(f"Python Executable           : {sys.executable}")
    print(f"Spark Master                : {spark.sparkContext.master}")
    print(f"Configured Driver Memory    : {cfg.get('driver_memory', '4g')}")
    print(f"Configured Executor Memory  : {cfg.get('executor_memory', '4g')}")
    print(f"Shuffle Partitions          : {cfg.get('shuffle_partitions', 8)}")
    print(f"Java Home                   : {java_home}")
    print(f"Hadoop Home (winutils)      : {hadoop_home}")
    print(f"Driver Host & BindAddress   : 127.0.0.1")
    print("=" * 60 + "\n")
    spark.stop()


if __name__ == "__main__":
    verify_spark_environment()
