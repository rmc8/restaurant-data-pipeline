import os
from datetime import datetime
from typing import Optional

import fire

from restaurant_data_pipeline.tabelog import Tabelog


def proc(
    url: str = "https://tabelog.com/tokyo/A1304/A130401/R5172/rstLst/?Srt=D&SrtT=rt&sort_mode=1&svd=20250217&svt=2000&svps=2",
    ua: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    skip: Optional[int] = None,
    limit: Optional[int] = None,
    test_mode: bool = False,
    file_name: str = "restaurant_data_{now:%Y%m%d%H%M%S}.csv",
):
    tl = Tabelog(
        base_url=url,
        ua=ua,
        test_mode=test_mode,
        skip=skip,
        limit=limit,
    )
    df = tl.scrape()
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, file_name.format(now=datetime.now()))
    df.write_csv(output_path)


def main():
    fire.Fire(proc)


if __name__ == "__main__":
    main()
