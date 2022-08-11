from flask import Blueprint, request
from .models import EstateTransaction


apiBlueprint = Blueprint("version1", __name__, url_prefix="/api/v1")

esTxBlueprint = Blueprint(
    "estateTransaction", __name__, url_prefix="/estate/transaction"
)

apiBlueprint.register_blueprint(esTxBlueprint)


@esTxBlueprint.route("/", methods=["GET"])
def list():
    """
    List transaction records of estate
    ---
    tags:
      - estate
    parameters:
      - name: offset
        in: query
        type: integer
        required: false
        default: 0
        description: offset from first record.
      - name: limit
        in: query
        type: integer
        required: false
        default: 10
        description: record number per page.
      - name: fields
        in: query
        type: string
        required: false
        description: column names, comma separated.
    responses:
      500:
        description: Server error
      200:
        description: Retrieve transaction record of estate
        schema:
          id: estateTransaction
          properties:
            主建物面積:
              type: number
            主要建材:
              type: string
            主要用途:
              type: string
            交易年月日:
              type: string
            交易標的:
              type: string
            交易筆棟數:
              type: string
            備註:
              type: string
            單價元平方公尺:
              type: integer
            土地位置建物門牌:
              type: string
            土地移轉總面積平方公尺:
              type: number
            建物型態:
              type: string
            建物現況格局-廳:
              type: integer
            建物現況格局-房:
              type: integer
            建物現況格局-衛:
              type: integer
            建物現況格局-隔間:
              type: string
            建物移轉總面積平方公尺:
              type: number
            建築完成年月:
              type: string
            有無管理組織:
              type: string
            移轉層次:
              type: string
            移轉編號:
              type: string
            編號:
              type: string
            總價元:
              type: integer
            總樓層數:
              type: string
            車位移轉總面積(平方公尺):
              type: number
            車位總價元:
              type: integer
            車位類別:
              type: string
            都市土地使用分區:
              type: string
            鄉鎮市區:
              type: string
            附屬建物面積:
              type: integer
            陽台面積:
              type: number
            電梯:
              type: string
            非都市土地使用分區:
              type: string
            非都市土地使用編定:
              type: string
    """
    fields = request.args.get("fields")
    limit = request.args.get("limit")
    offset = request.args.get("offset")

    limit = int(limit)
    offset = int(offset)

    data = EstateTransaction().list(
        fields=fields,
        limit=limit,
        offset=offset,
    )

    return data.to_dict(orient="records")


@esTxBlueprint.route("/<pk>", methods=["GET"])
def retrieve(pk: str):
    """
    Retrieve transaction record of estate
    ---
    tags:
      - estate
    parameters:
      - name: pk
        in: path
        type: string
        required: true
        description: primary key
      - name: fields
        in: query
        type: string
        required: false
        description: column names, comma separated.
    responses:
      500:
        description: Server error
      200:
        description: Retrieve transaction record of estate
        schema:
          id: estateTransaction
          properties:
            主建物面積:
              type: number
            主要建材:
              type: string
            主要用途:
              type: string
            交易年月日:
              type: string
            交易標的:
              type: string
            交易筆棟數:
              type: string
            備註:
              type: string
            單價元平方公尺:
              type: integer
            土地位置建物門牌:
              type: string
            土地移轉總面積平方公尺:
              type: number
            建物型態:
              type: string
            建物現況格局-廳:
              type: integer
            建物現況格局-房:
              type: integer
            建物現況格局-衛:
              type: integer
            建物現況格局-隔間:
              type: string
            建物移轉總面積平方公尺:
              type: number
            建築完成年月:
              type: string
            有無管理組織:
              type: string
            移轉層次:
              type: string
            移轉編號:
              type: string
            編號:
              type: string
            總價元:
              type: integer
            總樓層數:
              type: string
            車位移轉總面積(平方公尺):
              type: number
            車位總價元:
              type: integer
            車位類別:
              type: string
            都市土地使用分區:
              type: string
            鄉鎮市區:
              type: string
            附屬建物面積:
              type: integer
            陽台面積:
              type: number
            電梯:
              type: string
            非都市土地使用分區:
              type: string
            非都市土地使用編定:
              type: string
    """
    fields = request.args.get("fields")
    pk = int(pk)

    data = EstateTransaction().retrieve(pk=pk, fields=fields)

    return data.to_dict()
