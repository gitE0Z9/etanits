import os
import requests
from loguru import logger


class Scraper:

    filenameTemplate = "{rootDirectory}/{regionCode}_lvr_land_A.csv"
    urlTemplate = "https://plvr.land.moi.gov.tw//DownloadSeason?season={season}&fileName={regionCode}_lvr_land_A.csv"

    def __init__(self, rootDirectory: str, season: str, regionCode: str) -> None:
        self.rootDirectory = rootDirectory
        self.season = season
        self.regions = regionCode.split(",")

    def _getFilename(self, regionCode: str) -> str:
        return self.filenameTemplate.format(
            rootDirectory=self.rootDirectory,
            regionCode=regionCode,
        ).lower()

    def _getURL(self, season: str, regionCode: str) -> str:
        return self.urlTemplate.format(season=season, regionCode=regionCode)

    def download(self, season: str, regionCode: str) -> str:
        response = requests.get(self._getURL(season, regionCode))
        if response.status_code == 200:
            return response.text
        else:
            raise Exception(f"Download {self.url} failed.")

    def save(self, path: str, data: str) -> bool:
        try:
            os.makedirs(self.rootDirectory, exist_ok=True)
            with open(path, "w") as f:
                f.write(data)
            return True
        except Exception as e:
            logger.error(str(e))
            return False

    def run(self) -> None:
        for region in self.regions:
            data = self.download(self.season, region)
            filename = self._getFilename(region)
            status = self.save(filename, data)

            msg = f"{filename} {status}"

            if status:
                logger.info(msg)
            else:
                logger.error(msg)
