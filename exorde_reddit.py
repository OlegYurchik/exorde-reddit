import argparse
import asyncio
import dataclasses
import json
import logging
from datetime import datetime
from typing import Any, Awaitable, List

from playwright.async_api import async_playwright, Browser, ElementHandle, Page, TimeoutError


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
    POST_CREATED_AT_POPUP_SELECTOR: str = (
        "._2J_zB4R1FH2EjGMkQjedwc.u6HtAZu8_LKL721-EnKuR[data-popper-reference-hidden='false']"
    )
    POST_SCROLL_TRIES: int = 5

    COMMENT_SELECTOR: str = "._3sf33-9rVAO_v4y0pIW_CH"
    COMMENT_TEXT_SELECTOR: str = "._1qeIAgB0cPwnLhDF9XSiJM"
    COMMENT_CREATED_AT_SELECTOR: str = "._3yx4Dn0W3Yunucf5sVJeFU"
    COMMENT_CREATED_AT_POPUP_SELECTOR: str = (
        "._2J_zB4R1FH2EjGMkQjedwc.u6HtAZu8_LKL721-EnKuR[data-popper-reference-hidden='false']"
    )
    COMMENT_SCROLL_TRIES: int = 5

    SCROLL_DELAY_SECONDS: float = 5
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

            element_handles = await page.locator(self.POST_SELECTOR).element_handles()
            element_handles = element_handles[len(posts):]
            for element_handle in element_handles:
                await element_handle.scroll_into_view_if_needed()

                post = await self.parse_post(page=page, element_handle=element_handle)
                if post.id in post_ids:
                    self.logger.warning("Post %s already scrapped.", post.id)
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
            await asyncio.sleep(self.SCROLL_DELAY_SECONDS)

        self.logger.info("All posts loaded.")
        if tasks:
            await asyncio.wait(tasks)
        self.logger.info("All comments loaded.")
        await page.close()
        return posts

    async def parse_post(self, page: Page, element_handle: ElementHandle) -> RedditPost:
        id = await element_handle.get_attribute("id")
        id = id.lstrip("t3_")
        
        subreddit = await element_handle.query_selector(self.POST_SUBREDDIT_SELECTOR)
        subreddit = await subreddit.inner_text()

        title = await element_handle.query_selector(self.POST_TITLE_SELECTOR)
        title = await title.inner_text()

        created_at_element = await element_handle.query_selector(self.POST_CREATED_AT_SELECTOR)
        await created_at_element.hover()
        await page.wait_for_selector(self.POST_CREATED_AT_POPUP_SELECTOR)
        created_at = await page.locator(self.POST_CREATED_AT_POPUP_SELECTOR).element_handle()
        created_at = await created_at.inner_text()
        created_at = " ".join(created_at.split()[:-3])
        created_at = datetime.strptime(created_at, "%a, %b %d, %Y, %I:%M:%S %p").isoformat()

        return RedditPost(
            id=id,
            title=title,
            subreddit=subreddit,
            created_at=created_at,
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

            element_handles = await page.locator(self.COMMENT_SELECTOR).element_handles()
            element_handles = element_handles[len(comments):]
            for element_handle in element_handles:
                await element_handle.scroll_into_view_if_needed()

                comment = await self.parse_comment(page=page, element_handle=element_handle)
                if comment.id in comment_ids:
                    self.logger.warning("Comment %s already scrapped.", comment.id)
                    continue

                tries = 0
                have_new_comments = True
                comment_ids.add(comment.id)
                comments.append(comment)

            self.logger.info("Found %d comments for post (subreddit=%s; id=%s)", len(comments),
                             post.subreddit, post.id)
            await asyncio.sleep(self.SCROLL_DELAY_SECONDS)

        await page.close()
        post.comments = comments
        return comments

    async def parse_comment(self, page: Page, element_handle: ElementHandle) -> RedditComment:
        id = await element_handle.get_attribute("id")
        id = id.lstrip("t1_")

        text = await element_handle.query_selector(self.COMMENT_TEXT_SELECTOR)
        text = "" if text is None else await text.inner_text()

        created_at_element = await element_handle.query_selector(self.COMMENT_CREATED_AT_SELECTOR)
        await created_at_element.hover()
        await page.wait_for_selector(self.COMMENT_CREATED_AT_POPUP_SELECTOR)
        created_at = await page.locator(self.COMMENT_CREATED_AT_POPUP_SELECTOR).element_handle()
        created_at = await created_at.inner_text()
        created_at = " ".join(created_at.split()[:-3])
        created_at = datetime.strptime(created_at, "%a, %b %d, %Y, %I:%M:%S %p").isoformat()

        return RedditComment(id=id, text=text, created_at=created_at)


def run(start_datetime: int, *keywords: str) -> str:
    scrapper = RedditScrapper(*keywords)
    posts = asyncio.run(scrapper.run())
    posts = [dataclasses.asdict(post) for post in posts]

    return json.dumps(posts)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

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
