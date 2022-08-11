from typing import List
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.mapping import CITY_MAPPING
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from utils.convert import chinese2int
from loguru import logger

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class Reporter:
    def __init__(self, src: str, dst: str) -> None:
        self.src = src
        self.dst = dst
        self.session = SparkSession.builder.getOrCreate()

    def load(self, path: str) -> DataFrame:
        return self.session.read.csv(path, header=True, inferSchema=True)

    def transform(self, df: DataFrame) -> DataFrame:
        # pre-action: add city
        df = self._preAction(df)

        # clean
        df = self._clean(df, keepColumns=["city"])

        # transform columns
        df = self._transform(df)

        # filter
        df = self._filter(df)

        # create relation dict
        output = self._output(df)

        return output

    def save(self, df: DataFrame) -> None:
        df = df.withColumn("random", F.rand())

        # log
        rows = df.select("city", "random").sort("random", ascending=True).collect()
        for row in rows:
            if row.random > 0.5:
                logger.info(f"{row.city} into result-part1.json")
            else:
                logger.info(f"{row.city} into result-part2.json")

        # one
        df.filter(df.random > 0.5).drop("random").write.json(
            os.path.join(self.dst, "result-part1.json"), mode="Overwrite"
        )
        # two
        df.filter(df.random <= 0.5).drop("random").write.json(
            os.path.join(self.dst, "result-part2.json"), mode="Overwrite"
        )

    def run(self) -> None:
        # load csv files
        df = self.load(self.src)

        # transform to designed form
        df = self.transform(df)

        # save to json
        self.save(df)

    def _preAction(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "city",
            F.upper(
                F.regexp_extract(
                    F.input_file_name(),
                    "([a-z])_lvr_land",
                    1,
                )
            ),
        ).replace(CITY_MAPPING, subset=["city"])

    def _clean(self, df: DataFrame, keepColumns: List[str] = []) -> DataFrame:
        # remove english line
        df = df.filter(F.col("交易標的") != "transaction sign")

        # select columns
        df = df.select(
            F.col("鄉鎮市區").alias("district"),
            F.col("建物型態").alias("building_state"),
            F.col("總樓層數").alias("total_floor_number"),
            F.col("主要用途").alias("main_use"),
            F.col("交易年月日").alias("transaction_date"),
            *keepColumns,
        )

        return df

    def _transform(self, df: DataFrame) -> DataFrame:
        df = df.withColumns(
            {
                "building_state": F.when(
                    df.building_state.contains("住宅大樓"), "住宅大樓"
                ).otherwise(df.building_state),
                "total_floor_number": chinese2int(df.total_floor_number),
                "transaction_date": F.to_date(
                    df.transaction_date.cast(T.IntegerType())
                    .__add__(F.lit(19110000))
                    .cast(T.StringType()),
                    "yyyymmdd",
                ),
                "events": F.struct("building_state", "district"),
            }
        )

        return df

    def _filter(self, df: DataFrame) -> DataFrame:
        df = df.filter(
            (df.main_use == "住家用")
            & (df.building_state == "住宅大樓")
            & (df.total_floor_number >= 13)
        )

        return df

    def _output(self, df: DataFrame) -> DataFrame:
        df = (
            df.groupby("city", "transaction_date")
            .agg(
                F.struct(
                    F.col("transaction_date").alias("date"),
                    F.collect_list("events").alias("events"),
                ).alias("time_slots")
            )
            .sort(F.desc("transaction_date"))
            .drop("transaction_date")
            .groupby("city")
            .agg(F.collect_list("time_slots").alias("time_slots"))
        )

        return df
