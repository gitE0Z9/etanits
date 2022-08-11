import re
from typing import Optional

import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.IntegerType())
def chinese2int(val: Optional[str]):
    val = val or ""
    digit = "零一二三四五六七八九十"
    floor = re.search("([一二三四五六七八九十]+)層", val)

    if floor:
        floor = floor.group(1)
        sum = 0
        for i in range(0, len(floor), 2):
            sum += digit.find(floor[i]) * 10 ** (i // 2)
        return sum
    else:
        return


# @F.pandas_udf(
#     T.ArrayType(T.MapType(T.StringType(), T.StringType())),
#     returnType=F.PandasUDFType.GROUPED_MAP,
# )
# def one_to_many_list(pdf):
#     return pdf.assign(
#         output=pdf[["district", "building_state"]].to_dict(orient="records")
#     )
