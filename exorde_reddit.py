import argparse
import asyncio
import dataclasses
import json
import logging
from datetime import datetime
from typing import Any, Awaitable, List

from playwright.async_api import async_playwright, Browser, ElementHandle


@dataclasses.dataclass
class RedditComment:
    id: str
    text: str
    created_at: str


@dataclasses.dataclass
class RedditPost:
    id: str
    subreddit: str
    title: str
    created_at: str
    comments: List[RedditComment]


async def with_semaphore(function: Awaitable, semaphore: asyncio.Semaphore) -> Any:
    async with semaphore:
        try:
            return await function
        except:
            pass


class RedditScrapper:
    BASE_URL: str = "https://reddit.com"
    
    POST_SELECTOR: str = ".Post"
    POST_SUBREDDIT_SELECTOR: str = "._3ryJoIoycVkA88fy40qNJc"
    POST_TITLE_SELECTOR: str = ".SQnoC3ObvgnGjWt90zD9Z"
    POST_CREATED_AT_SELECTOR: str = "._2VF2J19pUIMSLJFky-7PEI"
    POST_SCROLL_TRIES: int = 5

    COMMENT_SELECTOR = "._3sf33-9rVAO_v4y0pIW_CH"
    COMMENT_TEXT_SELECTOR = "._1qeIAgB0cPwnLhDF9XSiJM"
    COMMENT_CREATED_AT_SELECTOR = "._3yx4Dn0W3Yunucf5sVJeFU"
    COMMENT_SCROLL_TRIES: int = 5

    SCROLL_SIZE_PIXELS: int = 15000
    SCROLL_DELAY_SECONDS: float = 1
    MAX_CONCURRENT_TASK: int = 5

    def __init__(self, *keywords: str, debug: bool = False):
        self.browser: Browser | None = None
        self.semaphore = asyncio.Semaphore(value=self.MAX_CONCURRENT_TASK)
        self.query = " ".join(keywords)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

    async def run(self) -> List[RedditPost]:
        async with async_playwright() as playwright:
            self.browser = await playwright.chromium.launch()
            return await self.search()

    async def search(self) -> List[RedditPost]:
        page = await self.browser.new_page()
        await page.goto(f"{self.BASE_URL}/search?q={self.query}")
        self.logger.debug("Load search page.")

        posts = []
        post_ids = set()
        tasks = []
        have_new_posts = True
        tries = 0

        while have_new_posts or tries < self.POST_SCROLL_TRIES:
            if not have_new_posts:
                self.logger.debug("Have no found new posts, attempt %d", tries + 1)

            have_new_posts = False
            tries += 1
            await page.mouse.wheel(0, self.SCROLL_SIZE_PIXELS)
            await asyncio.sleep(self.SCROLL_DELAY_SECONDS)

            for element_handle in await page.locator(self.POST_SELECTOR).element_handles():
                post = await self.parse_post(element_handle=element_handle)
                if post.id in post_ids:
                    continue

                tries = 0
                have_new_posts = True
                post_ids.add(post.id)
                posts.append(post)

                task = asyncio.create_task(with_semaphore(
                    function=self.search_comments(post=post),
                    semaphore=self.semaphore,
                ))
                tasks.append(task)
        
            self.logger.info("Found %d reddit posts.", len(posts))

        self.logger.info("All posts loaded.")
        if tasks:
            await asyncio.wait(tasks)
        self.logger.info("All comments loaded.")
        await page.close()
        return posts

    async def parse_post(self, element_handle: ElementHandle) -> RedditPost:
        id = await element_handle.get_attribute("id")
        id = id.lstrip("t3_")
        
        subreddit = await element_handle.query_selector(self.POST_SUBREDDIT_SELECTOR)
        subreddit = await subreddit.inner_text()

        title = await element_handle.query_selector(self.POST_TITLE_SELECTOR)
        title = await title.inner_text()
        
        # TODO: Getting correct created_at
        created_at = await element_handle.query_selector(self.POST_CREATED_AT_SELECTOR)

        # TODO: created_at it is not correct, need get this value from element_handler
        return RedditPost(
            id=id,
            title=title,
            subreddit=subreddit,
            created_at=datetime.now().isoformat(),
            comments=[],
        )

    async def search_comments(self, post: RedditPost) -> List[RedditComment]:
        page = await self.browser.new_page()
        await page.goto(f"{self.BASE_URL}/{post.subreddit}/comments/{post.id}")
        self.logger.debug("Load post page: subreddit=%s; id=%s.", post.subreddit, post.id)

        comments = []
        comment_ids = set()
        have_new_comments = True
        tries = 0

        while have_new_comments or tries < self.COMMENT_SCROLL_TRIES:
            if not have_new_comments:
                self.logger.debug(
                    "Have no found new comments for post (subreddit=%s; id=%s), attempt %d",
                    post.subreddit,
                    post.id,
                    tries + 1,
                )

            have_new_comments = False
            tries += 1
            await page.mouse.wheel(0, self.SCROLL_SIZE_PIXELS)
            await asyncio.sleep(self.SCROLL_DELAY_SECONDS)

            for element_handle in await page.locator(self.COMMENT_SELECTOR).element_handles():
                comment = await self.parse_comment(element_handle=element_handle)
                if comment.id in comment_ids:
                    continue

                tries = 0
                have_new_comments = True
                comment_ids.add(comment.id)
                comments.append(comment)

            self.logger.info("Found %d comments for post (subreddit=%s; id=%s)", len(comments),
                             post.subreddit, post.id)

        await page.close()
        post.comments = comments
        return comments

    async def parse_comment(self, element_handle: ElementHandle) -> RedditComment:
        id = await element_handle.get_attribute("id")
        id = id.lstrip("t1_")

        text = await element_handle.query_selector(self.COMMENT_TEXT_SELECTOR)
        text = "" if text is None else await text.inner_text()

        # TODO: Getting correct created_at
        created_at = await element_handle.query_selector(self.COMMENT_CREATED_AT_SELECTOR)

        # TODO: created_at it is not correct, need get this value from element_handler
        return RedditComment(id=id, text=text, created_at=datetime.now().isoformat())


def run(start_datetime: int, *keywords: str) -> str:
    scrapper = RedditScrapper(*keywords)
    posts = asyncio.run(scrapper.run())
    posts = [dataclasses.asdict(post) for post in posts]

    return json.dumps(posts)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        prog="Exorde Reddit Scrapper",
        description="Exorde network module for scrapping posts and comments from Reddit",
    )
    parser.add_argument("keywords", type=str, nargs="+", help="Search keywords.")
    parser.add_argument("-o", "--output", type=str, default="output.json", help="Output file name.")
    args = parser.parse_args()

    output = run(1, *args.keywords)
    with open(args.output, "wt") as output_file:
        output_file.write(output)
