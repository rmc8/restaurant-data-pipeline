import os
import re
import time
import random
import logging
from typing import List, Optional, TypedDict
from urllib.parse import urljoin

import httpx
import polars as pl
from tqdm import tqdm
from bs4 import BeautifulSoup


def setup_logger() -> logging.Logger:
    # ロガーの作成
    logger = logging.getLogger("tabelog_scraper")
    logger.setLevel(logging.INFO)

    # ファイルハンドラーの設定
    file_handler = logging.FileHandler("tabelog_scraper.log", encoding="utf-8")
    file_handler.setLevel(logging.INFO)

    # コンソールハンドラーの設定
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # フォーマッターの作成
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ハンドラーにフォーマッターを設定
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # ロガーにハンドラーを追加
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = setup_logger()


class RestaurantItem(TypedDict):
    restaurant_id: int
    url: str


class Tabelog:
    UPPER_LIMIT = 60
    TEST_MODE_LENGTH = 5

    def __init__(
        self,
        base_url: str,
        ua: str,
        test_mode: bool = False,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        logger: logging.Logger = logger,
    ):
        self.base_url = base_url
        self.test_mode = test_mode
        self.begin_page = skip or 1
        self.limit = limit or self.UPPER_LIMIT
        self.headers = {"User-Agent": ua}
        self.logger = logger
        self.restaurant_id = 0
        self.client = httpx.Client(
            headers=self.headers,
            timeout=httpx.Timeout(
                connect=30.0,
                read=30.0,
                write=30.0,
                pool=30.0,
            ),
            follow_redirects=True,
        )

    def __del__(self):
        if hasattr(self, "client"):
            self.client.close()

    @staticmethod
    def sleep():
        time.sleep(random.uniform(1.0, 3.0))

    def _scrape_urls(self, url: str) -> list:
        try:
            res = self.client.get(url, headers=self.headers)
            res.raise_for_status()
        except httpx.HTTPStatusError as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return []
        bs = BeautifulSoup(res.content, "lxml")
        soup_a_list = bs.find_all("a", class_="list-rst__rst-name-target")
        if not soup_a_list:
            logging.info("No data found on page {page_num}")
            return []
        if self.test_mode:
            target_restaurants = soup_a_list[: self.TEST_MODE_LENGTH]
        else:
            target_restaurants = soup_a_list
        restaurant_list: List[RestaurantItem] = []
        for soup_a in target_restaurants:
            item_url = soup_a.get("href")
            if not item_url:
                continue
            restaurant_list.append(
                {
                    "restaurant_id": self.restaurant_id,
                    "url": item_url,
                }
            )
            self.restaurant_id += 1
        return restaurant_list

    @staticmethod
    def _get_store(bs: BeautifulSoup) -> Optional[str]:
        elm = bs.select_one("h2.display-name")
        if elm is None:
            return
        return elm.get_text(strip=True)

    @staticmethod
    def _get_genre(bs: BeautifulSoup) -> Optional[str]:
        elm = bs.find("th", text="ジャンル")
        if elm is None:
            return
        genre_tag = elm.find_next_sibling("td").find("span")
        if genre_tag:
            return genre_tag.get_text(strip=True)

    @staticmethod
    def _get_score(bs: BeautifulSoup) -> Optional[float]:
        rating_score_tag = bs.find("b", class_="c-rating__val")
        if not rating_score_tag or not rating_score_tag.span:
            return
        rating_score = rating_score_tag.span.get_text(strip=True)
        if rating_score == "-":
            return
        return float(rating_score)

    @staticmethod
    def _get_budget(bs: BeautifulSoup, is_lunch: bool = False) -> Optional[str]:
        elm = bs.select_one(".rdheader-budget")
        for p_elm in elm.find_all("p"):
            i_elm = p_elm.find("i")
            if i_elm is None:
                continue
            if (i_elm.get("aria-label") == "Lunch" and is_lunch) or (
                i_elm.get("aria-label") == "Dinner" and not is_lunch
            ):
                s_elm = p_elm.find("span")
                if s_elm is not None:
                    return s_elm.get_text(strip=True)

    @staticmethod
    def _get_review_count(bs: BeautifulSoup) -> Optional[int]:
        elm = bs.select_one(".rdheader-rating__review")
        if elm is None:
            return
        count_elm = elm.select_one("em.num")
        if count_elm is None:
            return
        return int(count_elm.get_text(strip=True))

    @staticmethod
    def _get_bookmark_count(bs: BeautifulSoup) -> Optional[int]:
        elm = bs.select_one(".rdheader-rating__hozon")
        if elm is None:
            return
        count_elm = elm.select_one("em.num")
        if count_elm is None:
            return
        return int(count_elm.get_text(strip=True))

    def _scrape_item(self, url_list: List[RestaurantItem]) -> pl.polars:
        if not url_list:
            return pl.DataFrame()
        data_list = []
        for url_dict in tqdm(url_list):
            item_url = url_dict["url"]
            try:
                res = self.client.get(item_url, headers=self.headers)
                self.sleep()
                res.raise_for_status()
            except httpx.HTTPStatusError as e:
                self.logger.error(f"HTTP error occurred: {e}")
                data_list.append(
                    {
                        **url_dict,
                        "status": res.status_code,
                        "error": e,
                    }
                )
                continue
            bs = BeautifulSoup(res.content, "lxml")
            data = {
                **url_dict,
                "name": Tabelog._get_store(bs),
                "genre": Tabelog._get_genre(bs),
                "score": Tabelog._get_score(bs),
                "budget_of_lunch": Tabelog._get_budget(bs, is_lunch=True),
                "budget_of_dinner": Tabelog._get_budget(bs, is_lunch=False),
                "review_count": Tabelog._get_review_count(bs),
                "bookmark_count": Tabelog._get_bookmark_count(bs),
                "status": res.status_code,
                "error": None,
            }
            data_list.append(data)
        return pl.DataFrame(data_list)

    def scrape(self) -> pl.DataFrame:
        page_num = self.begin_page
        url_list: List[RestaurantItem] = []
        base_url: str = self.base_url.split("?")[0]
        params = self.base_url.split("?")[-1]
        while True:
            self.logger.info(f"Scraping page {page_num}...")
            url = urljoin(base_url, str(page_num) + "/")
            url_with_params = f"{url}?{params}"
            restaurant_url_list = self._scrape_urls(url_with_params)
            # restaurantの情報を追加し、情報がなければクローリングを終了する
            if restaurant_url_list:
                url_list.extend(restaurant_url_list)
            else:
                self.logger.info(
                    f"No more restaurants found on page {page_num}. Exiting..."
                )
                break
            # テストモードの場合、クローリングを終える
            if self.test_mode:
                self.logger.debug("Test mode is enabled. Stopping scraping.")
                break
            page_num += 1
            # ページ数の上限が来たらクローリングを終了する
            if page_num > self.limit:
                self.logger.info("Reached the limit of pages to scrape. Exiting...")
                break
            time.sleep(1)
        self.logger.info(f"Scraping {len(url_list)} restaurants in total.")
        return self._scrape_item(url_list)
